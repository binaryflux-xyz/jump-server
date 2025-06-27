package xyz.binaryflux.jumpserver;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.yaml.snakeyaml.Yaml;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.*;
import io.netty.channel.socket.nio.*;
import io.netty.handler.ssl.*;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.*;
import java.net.InetSocketAddress;
import java.nio.file.*;
import java.security.KeyStore;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;

public class JumpServer {

    /* ---------- Config POJOs ---------- */

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class TLSProfile {
        public String ca_cert;
        public String client_cert;
        public String client_key;
        public String server_name;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class Listen {
        public String ip;
        public int    port;
        public String protocol;   // "tcp" | "udp"
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class Forward {
        public String host;
        public int    port;
        public Boolean tls = null;  // null=default(true), false=plain
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class Route {
        public String  name;
        public Listen  listen;
        public Forward forward;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class Config {
        public TLSProfile   tls_profile;
        public List<Route>  routes;
        public BatchConfig  batch_config = new BatchConfig();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class BatchConfig {
        public int max_batch_size = 1000;        // Maximum number of records per batch
        public int max_batch_bytes = 1024 * 1024; // Maximum bytes per batch (1MB)
        public long flush_interval_ms = 1000;    // Flush interval in milliseconds
        public boolean enable_compression = true; // Enable GZIP compression
    }

    /* ---------- Bootstrap ---------- */

    public static void main(String[] args) throws Exception {

        String cfgFile = args.length > 0 ? args[0] : "config.yaml";
        Config cfg = loadConfig(cfgFile);

        // prepare one SSL context (client side) now
        SslContext sslCtx = buildClientSslContext(cfg.tls_profile);

        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        for (Route r : cfg.routes) {
            if ("tcp".equalsIgnoreCase(r.listen.protocol)) {
                startTcpRoute(r, sslCtx, bossGroup, workerGroup, cfg.batch_config);
            } else if ("udp".equalsIgnoreCase(r.listen.protocol)) {
                startUdpRoute(r, sslCtx, workerGroup, cfg.batch_config);
            } else {
                System.err.printf("[%s] Unknown protocol %s%n", r.name, r.listen.protocol);
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }));
    }

    /* ---------- TCP ---------- */

    private static void startTcpRoute(Route r, SslContext sslCtx,
                                      EventLoopGroup boss, EventLoopGroup workers, BatchConfig batchConfig) {

        new ServerBootstrap()
                .group(boss, workers)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new TcpRelayHandler(r, sslCtx, batchConfig));
                    }
                })
                .bind(new InetSocketAddress(r.listen.ip, r.listen.port))
                .addListener((ChannelFuture f) -> logBind(r, f));
    }

    private static final class TcpRelayHandler extends ChannelInboundHandlerAdapter {
        private final Route route;
        private final SslContext sslCtx;
        private Channel outbound;
        private final BatchConfig batchConfig;

        private static final int MAX_RETRIES = 5;
        private static final long RETRY_DELAY_MS = 2000;
        private int attempts = 0;

        TcpRelayHandler(Route route, SslContext sslCtx, BatchConfig batchConfig) {
            this.route = route;
            this.sslCtx = sslCtx;
            this.batchConfig = batchConfig;
        }

        @Override public void channelActive(ChannelHandlerContext ctx) {
            connect(ctx);
        }
        
        private void connect(ChannelHandlerContext ctx) {
            Bootstrap b = new Bootstrap()
                    .group(ctx.channel().eventLoop())
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override protected void initChannel(SocketChannel ch) {
                            if (useTls()) ch.pipeline().addLast(sslCtx.newHandler(ch.alloc(),
                                    route.forward.host, route.forward.port));
                            ch.pipeline().addLast(new BatchedForwarder(ctx.channel(), batchConfig));
                        }
                    });

            b.connect(route.forward.host, route.forward.port).addListener((ChannelFuture f) -> {
                if (f.isSuccess()) {
                    attempts = 0;
                    outbound = f.channel();
                    ctx.channel().read();
                } else {
                    if (++attempts <= MAX_RETRIES) {
                        ctx.executor().schedule(
                            () -> connect(ctx),
                            RETRY_DELAY_MS,
                            TimeUnit.MILLISECONDS);
                    } else {
                        ctx.close();
                    }
                }
            });
        }

        @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (outbound != null) {
                // Pass the message to the batched forwarder
                outbound.pipeline().get(BatchedForwarder.class).addToBatch(msg);
            }
        }

        @Override public void channelInactive(ChannelHandlerContext ctx) {
            if (outbound != null) outbound.close();
        }

        private boolean useTls() { return route.forward.tls == null || route.forward.tls; }
    }

    private static final class BatchedForwarder extends ChannelInboundHandlerAdapter {
        private final Channel inbound;
        private final BatchConfig batchConfig;
        private final ConcurrentLinkedQueue<ByteBuf> batchQueue = new ConcurrentLinkedQueue<>();
        private int currentBatchSize = 0;
        private int currentBatchBytes = 0;
        private ScheduledFuture<?> flushTask;

        BatchedForwarder(Channel inbound, BatchConfig batchConfig) {
            this.inbound = inbound;
            this.batchConfig = batchConfig;
            scheduleFlush();
        }

        @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
            // Forward response data back to the inbound channel
            inbound.writeAndFlush(msg);
        }

        @Override public void channelInactive(ChannelHandlerContext ctx) {
            if (flushTask != null) {
                flushTask.cancel(false);
            }
            flushBatch(); // Final flush
            inbound.close();
        }

        public void addToBatch(Object msg) {
            if (msg instanceof ByteBuf) {
                ByteBuf buf = (ByteBuf) msg;
                int msgSize = buf.readableBytes();
                
                // Check if adding this message would exceed batch limits
                if (currentBatchSize >= batchConfig.max_batch_size || 
                    currentBatchBytes + msgSize >= batchConfig.max_batch_bytes) {
                    flushBatch();
                }
                
                // Add to batch
                batchQueue.offer(buf.retain()); // Retain to keep reference
                currentBatchSize++;
                currentBatchBytes += msgSize;
            }
        }

        private void scheduleFlush() {
            if (batchConfig.flush_interval_ms > 0) {
                flushTask = inbound.eventLoop().scheduleAtFixedRate(
                    this::flushBatch,
                    batchConfig.flush_interval_ms,
                    batchConfig.flush_interval_ms,
                    TimeUnit.MILLISECONDS
                );
            }
        }

        private void flushBatch() {
            if (batchQueue.isEmpty()) {
                return;
            }
            try {
                // Gather all records for splitting if needed
                java.util.List<ByteBuf> records = new java.util.ArrayList<>();
                ByteBuf msg;
                while ((msg = batchQueue.poll()) != null) {
                    records.add(msg);
                }
                sendBatchWithSplit(records);
                // Reset batch counters
                currentBatchSize = 0;
                currentBatchBytes = 0;
            } catch (Exception e) {
                System.err.printf("Error flushing batch: %s%n", e.getMessage());
            }
        }

        private void sendBatchWithSplit(java.util.List<ByteBuf> records) {
            if (records.isEmpty()) return;
            ByteBufAllocator alloc = inbound.alloc();
            ByteBuf batchData = alloc.buffer();
            try {
                for (ByteBuf buf : records) {
                    batchData.writeBytes(buf, buf.readerIndex(), buf.readableBytes());
                }
                ByteBuf compressedData = batchData;
                if (batchConfig.enable_compression && batchData.readableBytes() > 0) {
                    compressedData = compressData(batchData, alloc);
                    batchData.release();
                }
                final int recordCount = records.size();
                inbound.writeAndFlush(compressedData).addListener((ChannelFuture future) -> {
                    if (!future.isSuccess()) {
                        if (recordCount > 1) {
                            int mid = recordCount / 2;
                            java.util.List<ByteBuf> left = records.subList(0, mid);
                            java.util.List<ByteBuf> right = records.subList(mid, recordCount);
                            sendBatchWithSplit(new java.util.ArrayList<>(left));
                            sendBatchWithSplit(new java.util.ArrayList<>(right));
                        } else {
                            System.err.printf("Failed to send single record: %s%n", future.cause());
                            records.get(0).release();
                        }
                    } else {
                        for (ByteBuf buf : records) buf.release();
                    }
                });
            } catch (Throwable t) {
                batchData.release();
                for (ByteBuf buf : records) buf.release();
                System.err.printf("Error in sendBatchWithSplit: %s%n", t.getMessage());
            }
        }

        private ByteBuf compressData(ByteBuf data, ByteBufAllocator alloc) throws IOException {
            ByteBuf compressed = alloc.buffer();
            try (ByteBufOutputStream bbos = new ByteBufOutputStream(compressed);
                 GZIPOutputStream gzip = new GZIPOutputStream(bbos)) {
                byte[] buffer = new byte[data.readableBytes()];
                data.getBytes(data.readerIndex(), buffer);
                gzip.write(buffer);
                gzip.finish();
                return compressed;
            } catch (IOException e) {
                compressed.release();
                throw e;
            }
        }
    }

    /* ---------- UDP ---------- */

    private static void startUdpRoute(Route r, SslContext sslCtx, EventLoopGroup group, BatchConfig batchConfig) {

        new Bootstrap().group(group)
                .channel(NioDatagramChannel.class)
                .handler(new BatchedUdpHandler(r, sslCtx, batchConfig))
                .bind(new InetSocketAddress(r.listen.ip, r.listen.port))
                .addListener((ChannelFuture f) -> logBind(r, f));
    }

    private static final class BatchedUdpHandler extends SimpleChannelInboundHandler<DatagramPacket> {
        private final Route route;
        private final SslContext sslCtx;
        private final BatchConfig batchConfig;
        private final ConcurrentLinkedQueue<DatagramPacket> batchQueue = new ConcurrentLinkedQueue<>();
        private int currentBatchSize = 0;
        private int currentBatchBytes = 0;
        private ScheduledFuture<?> flushTask;
        private ChannelHandlerContext ctx;

        BatchedUdpHandler(Route route, SslContext sslCtx, BatchConfig batchConfig) {
            this.route = route;
            this.sslCtx = sslCtx;
            this.batchConfig = batchConfig;
        }

        @Override public void channelActive(ChannelHandlerContext ctx) {
            this.ctx = ctx;
            scheduleFlush();
        }

        @Override protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket pkt) {
            int msgSize = pkt.content().readableBytes();
            
            // Check if adding this message would exceed batch limits
            if (currentBatchSize >= batchConfig.max_batch_size || 
                currentBatchBytes + msgSize >= batchConfig.max_batch_bytes) {
                flushBatch(ctx);
            }
            
            // Add to batch (retain the content)
            batchQueue.offer(new DatagramPacket(pkt.content().retain(), pkt.sender()));
            currentBatchSize++;
            currentBatchBytes += msgSize;
        }

        @Override public void channelInactive(ChannelHandlerContext ctx) {
            if (flushTask != null) {
                flushTask.cancel(false);
            }
            flushBatch(ctx); // Final flush
        }

        private void scheduleFlush() {
            if (batchConfig.flush_interval_ms > 0 && ctx != null) {
                flushTask = ctx.channel().eventLoop().scheduleAtFixedRate(
                    () -> flushBatch(ctx),
                    batchConfig.flush_interval_ms,
                    batchConfig.flush_interval_ms,
                    TimeUnit.MILLISECONDS
                );
            }
        }

        private void flushBatch(ChannelHandlerContext ctx) {
            if (batchQueue.isEmpty()) {
                return;
            }
            try {
                // Gather all records for splitting if needed
                java.util.List<DatagramPacket> records = new java.util.ArrayList<>();
                DatagramPacket pkt;
                while ((pkt = batchQueue.poll()) != null) {
                    records.add(pkt);
                }
                sendBatchWithSplit(ctx, records);
                // Reset batch counters
                currentBatchSize = 0;
                currentBatchBytes = 0;
            } catch (Exception e) {
                System.err.printf("Error flushing UDP batch: %s%n", e.getMessage());
            }
        }

        private void sendBatchWithSplit(ChannelHandlerContext ctx, java.util.List<DatagramPacket> records) {
            if (records.isEmpty()) return;
            ByteBufAllocator alloc = ctx.alloc();
            ByteBuf batchData = alloc.buffer();
            try {
                for (DatagramPacket pkt : records) {
                    batchData.writeBytes(pkt.content(), pkt.content().readerIndex(), pkt.content().readableBytes());
                }
                ByteBuf compressedData = batchData;
                if (batchConfig.enable_compression && batchData.readableBytes() > 0) {
                    compressedData = compressData(batchData, alloc);
                    batchData.release();
                }
                sendBatchViaTcp(ctx, compressedData, records);
            } catch (Throwable t) {
                batchData.release();
                for (DatagramPacket pkt : records) pkt.content().release();
                System.err.printf("Error in sendBatchWithSplit (UDP): %s%n", t.getMessage());
            }
        }

        private void sendBatchViaTcp(ChannelHandlerContext ctx, ByteBuf data, java.util.List<DatagramPacket> records) {
            Bootstrap b = new Bootstrap()
                    .group(ctx.channel().eventLoop())
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override protected void initChannel(SocketChannel ch) {
                            if (useTls()) ch.pipeline().addLast(
                                    sslCtx.newHandler(ch.alloc(),
                                            route.forward.host, route.forward.port));
                        }
                        private boolean useTls() {
                            return route.forward.tls == null || route.forward.tls;
                        }
                    });
            final int recordCount = records.size();
            b.connect(route.forward.host, route.forward.port).addListener((ChannelFuture f) -> {
                if (f.isSuccess()) {
                    f.channel().writeAndFlush(data).addListener(future -> {
                        if (!future.isSuccess()) {
                            if (recordCount > 1) {
                                int mid = recordCount / 2;
                                java.util.List<DatagramPacket> left = records.subList(0, mid);
                                java.util.List<DatagramPacket> right = records.subList(mid, recordCount);
                                sendBatchWithSplit(ctx, new java.util.ArrayList<>(left));
                                sendBatchWithSplit(ctx, new java.util.ArrayList<>(right));
                            } else {
                                System.err.printf("Failed to send single UDP record: %s%n", future.cause());
                                records.get(0).content().release();
                            }
                        } else {
                            for (DatagramPacket pkt : records) pkt.content().release();
                        }
                    });
                } else {
                    data.release();
                    for (DatagramPacket pkt : records) pkt.content().release();
                }
            });
        }

        private ByteBuf compressData(ByteBuf data, ByteBufAllocator alloc) throws IOException {
            ByteBuf compressed = alloc.buffer();
            try (ByteBufOutputStream bbos = new ByteBufOutputStream(compressed);
                 GZIPOutputStream gzip = new GZIPOutputStream(bbos)) {
                byte[] buffer = new byte[data.readableBytes()];
                data.getBytes(data.readerIndex(), buffer);
                gzip.write(buffer);
                gzip.finish();
                return compressed;
            } catch (IOException e) {
                compressed.release();
                throw e;
            }
        }
    }

    /* ---------- Utilities ---------- */

    private static Config loadConfig(String file) throws IOException {
        try (Reader r = Files.newBufferedReader(Path.of(file))) {
            return new Yaml().loadAs(r, Config.class);
        }
    }

    private static SslContext buildClientSslContext(TLSProfile tls) throws Exception {
        // KeyStore with client cert/key
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        ks.setKeyEntry("key", PemUtil.loadPrivateKey(tls.client_key),
                new char[0], new java.security.cert.Certificate[]{ PemUtil.loadCert(tls.client_cert) });

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(
                KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, new char[0]);

        // Trust store
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm());
        KeyStore ts = KeyStore.getInstance("JKS");
        ts.load(null);
        ts.setCertificateEntry("ca", PemUtil.loadCert(tls.ca_cert));
        tmf.init(ts);

        return SslContextBuilder.forClient()
                .keyManager(kmf)
                .trustManager(tmf)
                .protocols("TLSv1.3", "TLSv1.2")
                .build();
    }

    private static void logBind(Route r, ChannelFuture f) {
        if (f.isSuccess())
            System.out.printf("[%s] listening %s/%d (%s)%n",
                    r.name, r.listen.ip, r.listen.port, r.listen.protocol);
        else
            System.err.printf("[%s] bind failed: %s%n", r.name, f.cause());
    }
}