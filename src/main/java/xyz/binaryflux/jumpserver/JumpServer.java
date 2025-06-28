package xyz.binaryflux.jumpserver;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.yaml.snakeyaml.Yaml;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import org.xerial.snappy.Snappy;

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
        public String protocol = "tcp";  // "tcp" | "udp" (default: tcp)
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
        public boolean enable_compression = true; // Enable Snappy compression
        public boolean enable_batching = true;    // Enable batching (default true)
        
        // Retry configuration
        public int max_connection_retries = 120;  // Maximum connection retry attempts (0 = infinite)
        public long connection_retry_delay_ms = 2000; // Delay between connection retries
        public int max_send_retries = 3;         // Maximum send retry attempts
        public long send_retry_delay_ms = 1000;  // Delay between send retries
        public boolean persistent_reconnection = true; // Keep trying to reconnect after disconnection
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
        private ChannelHandlerContext clientCtx;
        private ScheduledFuture<?> reconnectTask;
        private ScheduledFuture<?> cleanupTask;
        private boolean isReconnecting = false;
        private final ConcurrentLinkedQueue<ByteBuf> pendingMessages = new ConcurrentLinkedQueue<>();

        TcpRelayHandler(Route route, SslContext sslCtx, BatchConfig batchConfig) {
            this.route = route;
            this.sslCtx = sslCtx;
            this.batchConfig = batchConfig;
        }

        @Override public void channelActive(ChannelHandlerContext ctx) {
            this.clientCtx = ctx;
            connect(ctx);
        }
        
        private void connect(ChannelHandlerContext ctx) {
            if (isReconnecting) {
                System.err.printf("[DEBUG] Already attempting to reconnect, skipping%n");
                return;
            }
            
            isReconnecting = true;
            System.err.printf("[DEBUG] Attempting to connect to %s:%d%n", route.forward.host, route.forward.port);
            
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
                    System.err.printf("[DEBUG] Successfully connected to backend%n");
                    isReconnecting = false;
                    outbound = f.channel();
                    
                    // Process any pending messages
                    processPendingMessages();
                    
                    // Add connection close listener
                    outbound.closeFuture().addListener((ChannelFuture closeFuture) -> {
                        System.err.printf("[DEBUG] Backend connection closed%n");
                        outbound = null;
                        
                        // Clean up any remaining pending messages
                        cleanupPendingMessages();
                        
                        if (batchConfig.persistent_reconnection && clientCtx != null && clientCtx.channel().isActive()) {
                            scheduleReconnect();
                        }
                    });
                    
                    ctx.channel().read();
                } else {
                    System.err.printf("[DEBUG] Connection failed: %s%n", f.cause().getMessage());
                    isReconnecting = false;
                    
                    // Clean up pending messages on connection failure
                    cleanupPendingMessages();
                    
                    if (batchConfig.persistent_reconnection && clientCtx != null && clientCtx.channel().isActive()) {
                        scheduleReconnect();
                    } else {
                        ctx.close();
                    }
                }
            });
        }
        
        private void processPendingMessages() {
            if (outbound == null || !outbound.isActive()) {
                return;
            }
            
            System.err.printf("[DEBUG] Processing %d pending messages%n", pendingMessages.size());
            
            ByteBuf msg;
            while ((msg = pendingMessages.poll()) != null) {
                if (outbound != null && outbound.isActive()) {
                    if (!batchConfig.enable_batching) {
                        // Send immediately when batching is disabled
                        final ByteBuf finalMsg = msg;
                        outbound.writeAndFlush(msg.retain()).addListener((ChannelFuture future) -> {
                            if (!future.isSuccess()) {
                                System.err.printf("[DEBUG] Failed to send pending message: %s%n", future.cause().getMessage());
                            } else {
                                System.err.printf("[DEBUG] Successfully sent pending message%n");
                            }
                            finalMsg.release();
                        });
                    } else {
                        // Add to batch when batching is enabled
                        outbound.pipeline().get(BatchedForwarder.class).addToBatch(msg);
                    }
                } else {
                    // Put it back in the queue if connection is lost
                    pendingMessages.offer(msg);
                    break;
                }
            }
            
            // Cancel cleanup task if all messages were processed
            if (pendingMessages.isEmpty() && cleanupTask != null && !cleanupTask.isDone()) {
                cleanupTask.cancel(false);
                System.err.printf("[DEBUG] Cancelled cleanup task - all messages processed%n");
            }
        }
        
        private void cleanupPendingMessages() {
            System.err.printf("[DEBUG] Cleaning up %d pending messages%n", pendingMessages.size());
            ByteBuf msg;
            while ((msg = pendingMessages.poll()) != null) {
                msg.release();
            }
        }
        
        private void scheduleReconnect() {
            if (reconnectTask != null && !reconnectTask.isDone()) {
                return; // Already scheduled
            }
            
            System.err.printf("[DEBUG] Scheduling reconnection in %d ms%n", batchConfig.connection_retry_delay_ms);
            reconnectTask = clientCtx.executor().schedule(
                () -> connect(clientCtx),
                batchConfig.connection_retry_delay_ms,
                TimeUnit.MILLISECONDS
            );
        }

        @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
            System.out.println("[DEBUG] Msg received: " + msg);
            if (outbound != null && outbound.isActive()) {
                // Pass the message to the batched forwarder
                outbound.pipeline().get(BatchedForwarder.class).addToBatch(msg);
            } else {
                System.err.printf("[DEBUG] Backend not connected, queuing message for retry%n");
                // Queue message for retry when connection is restored
                if (msg instanceof ByteBuf) {
                    pendingMessages.offer(((ByteBuf) msg).retain());
                    
                    // Schedule cleanup after 30 seconds if messages aren't processed
                    if (cleanupTask == null || cleanupTask.isDone()) {
                        cleanupTask = ctx.executor().schedule(
                            this::cleanupPendingMessages,
                            30, TimeUnit.SECONDS
                        );
                    }
                }
            }
        }

        @Override public void channelInactive(ChannelHandlerContext ctx) {
            System.out.println("[DEBUG] Client disconnected: " + ctx.channel().remoteAddress());
            if (reconnectTask != null) {
                reconnectTask.cancel(false);
            }
            if (cleanupTask != null) {
                cleanupTask.cancel(false);
            }
            
            // Don't close outbound immediately, let pending messages be processed
            // Only close if we're not in the middle of connecting
            if (outbound != null && !isReconnecting) {
                outbound.close();
            }
            
            // Don't release pending messages immediately - let them be processed
            // They will be released when the connection is fully closed or timeout
        }

        private boolean useTls() { return route.forward.tls == null || route.forward.tls; }
    }

    private static final class BatchedForwarder extends ChannelInboundHandlerAdapter {
        private final Channel inbound;
        private final BatchConfig batchConfig;
        private final ConcurrentLinkedQueue<ByteBuf> batchQueue = new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<ByteBuf> retryQueue = new ConcurrentLinkedQueue<>();
        private int currentBatchSize = 0;
        private int currentBatchBytes = 0;
        private ScheduledFuture<?> flushTask;
        private ScheduledFuture<?> retryTask;

        BatchedForwarder(Channel inbound, BatchConfig batchConfig) {
            this.inbound = inbound;
            this.batchConfig = batchConfig;
            if (batchConfig.enable_batching) {
                scheduleFlush();
            }
            scheduleRetryTask();
        }

        @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (!batchConfig.enable_batching) {
                // Send immediately, no compression
                if (msg instanceof ByteBuf) {
                    inbound.writeAndFlush(msg);
                }
                return;
            }
            // Forward response data back to the inbound channel
            inbound.writeAndFlush(msg);
        }

        @Override public void channelInactive(ChannelHandlerContext ctx) {
            if (flushTask != null) {
                flushTask.cancel(false);
            }
            if (retryTask != null) {
                retryTask.cancel(false);
            }
            flushBatch(); // Final flush
            inbound.close();
        }

        public void queueForRetry(Object msg) {
            if (msg instanceof ByteBuf) {
                ByteBuf buf = (ByteBuf) msg;
                System.err.printf("[DEBUG] queueForRetry: Queuing %d bytes for retry%n", buf.readableBytes());
                retryQueue.offer(buf.retain());
            }
        }

        private void scheduleRetryTask() {
            if (batchConfig.send_retry_delay_ms > 0) {
                retryTask = inbound.eventLoop().scheduleAtFixedRate(
                    this::processRetryQueue,
                    batchConfig.send_retry_delay_ms,
                    batchConfig.send_retry_delay_ms,
                    TimeUnit.MILLISECONDS
                );
            }
        }

        private void processRetryQueue() {
            if (retryQueue.isEmpty()) {
                return;
            }
            
            System.err.printf("[DEBUG] processRetryQueue: Processing %d retry items%n", retryQueue.size());
            
            ByteBuf msg;
            while ((msg = retryQueue.poll()) != null) {
                addToBatch(msg);
            }
        }

        public void addToBatch(Object msg) {
            if (msg instanceof ByteBuf) {
                ByteBuf buf = (ByteBuf) msg;
                int msgSize = buf.readableBytes();
                
                System.err.printf("[DEBUG] addToBatch: Received message of %d bytes, current batch: %d records, %d bytes%n", 
                    msgSize, currentBatchSize, currentBatchBytes);
                
                if (!batchConfig.enable_batching) {
                    // Send immediately when batching is disabled with retry logic
                    System.err.printf("[DEBUG] addToBatch: Batching disabled, sending immediately%n");
                    sendWithRetry(buf, 0);
                    return;
                }
                
                // Check if adding this message would exceed batch limits
                if (currentBatchSize >= batchConfig.max_batch_size || 
                    currentBatchBytes + msgSize >= batchConfig.max_batch_bytes) {
                    System.err.printf("[DEBUG] addToBatch: Batch limits reached, flushing batch%n");
                    flushBatch();
                }
                
                // Add to batch
                batchQueue.offer(buf.retain()); // Retain to keep reference
                currentBatchSize++;
                currentBatchBytes += msgSize;
                
                System.err.printf("[DEBUG] addToBatch: Added to batch, new size: %d records, %d bytes%n", 
                    currentBatchSize, currentBatchBytes);
            }
        }

        private void sendWithRetry(ByteBuf buf, int attempt) {
            System.err.printf("[DEBUG] sendWithRetry: Attempting to send %d bytes to backend (attempt %d)%n", 
                buf.readableBytes(), attempt + 1);
            
            inbound.writeAndFlush(buf.retain()).addListener((ChannelFuture future) -> {
                if (!future.isSuccess()) {
                    System.err.printf("[DEBUG] sendWithRetry: Send failed (attempt %d) - %s%n", 
                        attempt + 1, future.cause().getMessage());
                    
                    if (attempt < batchConfig.max_send_retries) {
                        System.err.printf("[DEBUG] sendWithRetry: Retrying in %d ms%n", batchConfig.send_retry_delay_ms);
                        inbound.eventLoop().schedule(
                            () -> sendWithRetry(buf, attempt + 1),
                            batchConfig.send_retry_delay_ms,
                            TimeUnit.MILLISECONDS
                        );
                    } else {
                        System.err.printf("[DEBUG] sendWithRetry: Max retries reached, queuing for later retry%n");
                        retryQueue.offer(buf.retain());
                        buf.release();
                    }
                } else {
                    System.err.printf("[DEBUG] sendWithRetry: Successfully sent %d bytes to backend (attempt %d)%n", 
                        buf.readableBytes(), attempt + 1);
                    buf.release();
                }
            });
        }

        private void scheduleFlush() {
            if (batchConfig.enable_batching && batchConfig.flush_interval_ms > 0) {
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
                System.err.printf("[DEBUG] flushBatch: Queue is empty, nothing to flush%n");
                return;
            }
            
            System.err.printf("[DEBUG] flushBatch: Starting flush of %d records, %d bytes%n", 
                currentBatchSize, currentBatchBytes);
            
            try {
                // Gather all records for splitting if needed
                java.util.List<ByteBuf> records = new java.util.ArrayList<>();
                ByteBuf msg;
                while ((msg = batchQueue.poll()) != null) {
                    records.add(msg);
                }
                
                System.err.printf("[DEBUG] flushBatch: Gathered %d records for sending%n", records.size());
                sendBatchWithSplit(records);
                
                // Reset batch counters
                currentBatchSize = 0;
                currentBatchBytes = 0;
                System.err.printf("[DEBUG] flushBatch: Reset batch counters%n");
            } catch (Exception e) {
                System.err.printf("[DEBUG] flushBatch: Exception during flush: %s%n", e.getMessage());
                System.err.printf("Error flushing batch: %s%n", e.getMessage());
            }
        }

        private void sendBatchWithSplit(java.util.List<ByteBuf> records) {
            if (records.isEmpty()) return;
            
            System.err.printf("[DEBUG] sendBatchWithSplit: Starting with %d records%n", records.size());
            
            ByteBufAllocator alloc = inbound.alloc();
            ByteBuf batchData = alloc.buffer();
            try {
                // Log total size of records
                long totalSize = records.stream().mapToLong(ByteBuf::readableBytes).sum();
                System.err.printf("[DEBUG] sendBatchWithSplit: Total records size: %d bytes%n", totalSize);
                
                for (ByteBuf buf : records) {
                    batchData.writeBytes(buf, buf.readerIndex(), buf.readableBytes());
                }
                
                System.err.printf("[DEBUG] sendBatchWithSplit: Combined batch size: %d bytes%n", batchData.readableBytes());
                
                ByteBuf compressedData = batchData;
                if (batchConfig.enable_compression && batchData.readableBytes() > 0) {
                    System.err.printf("[DEBUG] sendBatchWithSplit: Compressing data...%n");
                    compressedData = compressData(batchData, alloc);
                    batchData.release();
                    System.err.printf("[DEBUG] sendBatchWithSplit: Compressed size: %d bytes (compression ratio: %.2f%%)%n", 
                        compressedData.readableBytes(), 
                        (100.0 * compressedData.readableBytes() / totalSize));
                } else {
                    System.err.printf("[DEBUG] sendBatchWithSplit: Compression disabled or empty data%n");
                }
                
                final int recordCount = records.size();
                final ByteBuf finalCompressedData = compressedData;
                System.err.printf("[DEBUG] sendBatchWithSplit: Sending %d records, %d bytes%n", 
                    recordCount, finalCompressedData.readableBytes());
                
                inbound.writeAndFlush(finalCompressedData).addListener((ChannelFuture future) -> {
                    if (!future.isSuccess()) {
                        Throwable cause = future.cause();
                        System.err.printf("[DEBUG] sendBatchWithSplit: Send failed - %s%n", cause.getMessage());
                        
                        boolean isClosed = cause instanceof java.nio.channels.ClosedChannelException ||
                                           (cause != null && cause.getClass().getSimpleName().contains("ClosedChannelException"));
                        
                        if (recordCount > 1 && !isClosed) {
                            System.err.printf("[DEBUG] sendBatchWithSplit: Splitting batch of %d records and retrying%n", recordCount);
                            int mid = recordCount / 2;
                            java.util.List<ByteBuf> left = records.subList(0, mid);
                            java.util.List<ByteBuf> right = records.subList(mid, recordCount);
                            sendBatchWithSplit(new java.util.ArrayList<>(left));
                            sendBatchWithSplit(new java.util.ArrayList<>(right));
                        } else {
                            if (isClosed) {
                                System.err.printf("[DEBUG] sendBatchWithSplit: Channel closed, writing %d records to retry file%n", recordCount);
                                RetryFileUtil.writeFailedBatch(records, batchConfig.enable_compression);
                            } else {
                                System.err.printf("[DEBUG] sendBatchWithSplit: Single record failed, logging error%n");
                            }
                            for (ByteBuf buf : records) buf.release();
                        }
                    } else {
                        System.err.printf("[DEBUG] sendBatchWithSplit: Successfully sent %d records, %d bytes%n", 
                            recordCount, finalCompressedData.readableBytes());
                        for (ByteBuf buf : records) buf.release();
                    }
                });
            } catch (Throwable t) {
                System.err.printf("[DEBUG] sendBatchWithSplit: Exception during processing: %s%n", t.getMessage());
                batchData.release();
                for (ByteBuf buf : records) buf.release();
                System.err.printf("Error in sendBatchWithSplit: %s%n", t.getMessage());
            }
        }

        private ByteBuf compressData(ByteBuf data, ByteBufAllocator alloc) throws IOException {
            if (!batchConfig.enable_compression) {
                return data.retain(); // Return uncompressed data
            }
            
            // Snappy compression
            byte[] input = new byte[data.readableBytes()];
            data.getBytes(data.readerIndex(), input);
            byte[] compressed = Snappy.compress(input);
            ByteBuf result = alloc.buffer(compressed.length);
            result.writeBytes(compressed);
            return result;
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
        private final ConcurrentLinkedQueue<DatagramPacket> retryQueue = new ConcurrentLinkedQueue<>();
        private int currentBatchSize = 0;
        private int currentBatchBytes = 0;
        private ScheduledFuture<?> flushTask;
        private ScheduledFuture<?> retryTask;
        private ChannelHandlerContext ctx;
        private Channel tcpConnection;
        private boolean isConnecting = false;

        BatchedUdpHandler(Route route, SslContext sslCtx, BatchConfig batchConfig) {
            this.route = route;
            this.sslCtx = sslCtx;
            this.batchConfig = batchConfig;
        }

        @Override public void channelActive(ChannelHandlerContext ctx) {
            this.ctx = ctx;
            if (batchConfig.enable_batching) {
                scheduleFlush();
            }
            scheduleRetryTask();
            // Establish persistent TCP connection
            establishTcpConnection();
        }

        private void establishTcpConnection() {
            if (isConnecting || (tcpConnection != null && tcpConnection.isActive())) {
                return;
            }
            
            // Check if we should use UDP forwarding
            if ("udp".equalsIgnoreCase(route.forward.protocol)) {
                System.err.printf("[DEBUG] UDP: Using UDP forwarding to %s:%d%n", 
                    route.forward.host, route.forward.port);
                return; // No TCP connection needed for UDP forwarding
            }
            
            isConnecting = true;
            System.err.printf("[DEBUG] UDP: Establishing persistent TCP connection to %s:%d%n", 
                route.forward.host, route.forward.port);
            
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
            
            b.connect(route.forward.host, route.forward.port).addListener((ChannelFuture f) -> {
                isConnecting = false;
                if (f.isSuccess()) {
                    tcpConnection = f.channel();
                    System.err.printf("[DEBUG] UDP: Persistent TCP connection established%n");
                    
                    // Add connection close listener
                    tcpConnection.closeFuture().addListener((ChannelFuture closeFuture) -> {
                        System.err.printf("[DEBUG] UDP: TCP connection closed, will reconnect%n");
                        tcpConnection = null;
                        // Reconnect after a delay
                        ctx.executor().schedule(
                            () -> establishTcpConnection(),
                            batchConfig.connection_retry_delay_ms,
                            TimeUnit.MILLISECONDS
                        );
                    });
                    
                    // Process any pending retry messages
                    processRetryQueue();
                } else {
                    System.err.printf("[DEBUG] UDP: TCP connection failed: %s%n", f.cause().getMessage());
                    // Retry connection after delay
                    ctx.executor().schedule(
                        () -> establishTcpConnection(),
                        batchConfig.connection_retry_delay_ms,
                        TimeUnit.MILLISECONDS
                    );
                }
            });
        }

        @Override protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket pkt) {
            if (!batchConfig.enable_batching) {
                // Send immediately, no compression, via TCP with retry
                sendSingleViaTcpWithRetry(ctx, pkt.content(), 0);
                return;
            }
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

        private void sendSingleViaTcpWithRetry(ChannelHandlerContext ctx, ByteBuf data, int attempt) {
            // Check if we should use UDP forwarding
            if ("udp".equalsIgnoreCase(route.forward.protocol)) {
                sendSingleViaUdpWithRetry(ctx, data, attempt);
                return;
            }
            
            if (tcpConnection != null && tcpConnection.isActive()) {
                // Use existing persistent connection
                tcpConnection.writeAndFlush(data.retain()).addListener((ChannelFuture writeFuture) -> {
                    if (!writeFuture.isSuccess()) {
                        System.err.printf("[DEBUG] UDP sendSingleViaTcpWithRetry: Send failed (attempt %d) - %s%n", 
                            attempt + 1, writeFuture.cause().getMessage());
                        
                        if (attempt < batchConfig.max_send_retries) {
                            System.err.printf("[DEBUG] UDP sendSingleViaTcpWithRetry: Retrying in %d ms%n", batchConfig.send_retry_delay_ms);
                            ctx.executor().schedule(
                                () -> sendSingleViaTcpWithRetry(ctx, data, attempt + 1),
                                batchConfig.send_retry_delay_ms,
                                TimeUnit.MILLISECONDS
                            );
                        } else {
                            System.err.printf("[DEBUG] UDP sendSingleViaTcpWithRetry: Max retries reached, queuing for later retry%n");
                            retryQueue.offer(new DatagramPacket(data.retain(), null));
                            data.release();
                        }
                    } else {
                        System.err.printf("[DEBUG] UDP sendSingleViaTcpWithRetry: Successfully sent %d bytes via persistent connection (attempt %d)%n", 
                            data.readableBytes(), attempt + 1);
                        data.release();
                    }
                });
            } else {
                // No persistent connection available, queue for retry
                System.err.printf("[DEBUG] UDP sendSingleViaTcpWithRetry: No persistent connection, queuing message%n");
                retryQueue.offer(new DatagramPacket(data.retain(), null));
                data.release();
                
                // Try to establish connection if not already connecting
                if (!isConnecting) {
                    establishTcpConnection();
                }
            }
        }

        private void sendSingleViaUdpWithRetry(ChannelHandlerContext ctx, ByteBuf data, int attempt) {
            System.err.printf("[DEBUG] UDP sendSingleViaUdpWithRetry: Sending %d bytes via UDP to %s:%d (attempt %d)%n", 
                data.readableBytes(), route.forward.host, route.forward.port, attempt + 1);
            
            // Create UDP channel for sending
            Bootstrap b = new Bootstrap()
                    .group(ctx.channel().eventLoop())
                    .channel(NioDatagramChannel.class);
            
            b.connect(route.forward.host, route.forward.port).addListener((ChannelFuture f) -> {
                if (f.isSuccess()) {
                    DatagramPacket packet = new DatagramPacket(data.retain(), 
                        new InetSocketAddress(route.forward.host, route.forward.port));
                    
                    f.channel().writeAndFlush(packet).addListener((ChannelFuture writeFuture) -> {
                        if (!writeFuture.isSuccess()) {
                            System.err.printf("[DEBUG] UDP sendSingleViaUdpWithRetry: Send failed (attempt %d) - %s%n", 
                                attempt + 1, writeFuture.cause().getMessage());
                            
                            if (attempt < batchConfig.max_send_retries) {
                                System.err.printf("[DEBUG] UDP sendSingleViaUdpWithRetry: Retrying in %d ms%n", batchConfig.send_retry_delay_ms);
                                ctx.executor().schedule(
                                    () -> sendSingleViaUdpWithRetry(ctx, data, attempt + 1),
                                    batchConfig.send_retry_delay_ms,
                                    TimeUnit.MILLISECONDS
                                );
                            } else {
                                System.err.printf("[DEBUG] UDP sendSingleViaUdpWithRetry: Max retries reached, queuing for later retry%n");
                                retryQueue.offer(new DatagramPacket(data.retain(), null));
                                data.release();
                            }
                        } else {
                            System.err.printf("[DEBUG] UDP sendSingleViaUdpWithRetry: Successfully sent %d bytes via UDP (attempt %d)%n", 
                                data.readableBytes(), attempt + 1);
                            data.release();
                        }
                        f.channel().close();
                    });
                } else {
                    System.err.printf("[DEBUG] UDP sendSingleViaUdpWithRetry: Connection failed (attempt %d) - %s%n", 
                        attempt + 1, f.cause().getMessage());
                    
                    if (attempt < batchConfig.max_send_retries) {
                        System.err.printf("[DEBUG] UDP sendSingleViaUdpWithRetry: Retrying in %d ms%n", batchConfig.send_retry_delay_ms);
                        ctx.executor().schedule(
                            () -> sendSingleViaUdpWithRetry(ctx, data, attempt + 1),
                            batchConfig.send_retry_delay_ms,
                            TimeUnit.MILLISECONDS
                        );
                    } else {
                        System.err.printf("[DEBUG] UDP sendSingleViaUdpWithRetry: Max retries reached, queuing for later retry%n");
                        retryQueue.offer(new DatagramPacket(data.retain(), null));
                        data.release();
                    }
                }
            });
        }

        private void scheduleRetryTask() {
            if (batchConfig.send_retry_delay_ms > 0) {
                retryTask = ctx.channel().eventLoop().scheduleAtFixedRate(
                    this::processRetryQueue,
                    batchConfig.send_retry_delay_ms,
                    batchConfig.send_retry_delay_ms,
                    TimeUnit.MILLISECONDS
                );
            }
        }

        private void processRetryQueue() {
            if (retryQueue.isEmpty()) {
                return;
            }
            
            System.err.printf("[DEBUG] UDP processRetryQueue: Processing %d retry items%n", retryQueue.size());
            
            DatagramPacket pkt;
            while ((pkt = retryQueue.poll()) != null) {
                if (!batchConfig.enable_batching) {
                    sendSingleViaTcpWithRetry(ctx, pkt.content(), 0);
                } else {
                    // Add back to batch queue
                    batchQueue.offer(pkt);
                    currentBatchSize++;
                    currentBatchBytes += pkt.content().readableBytes();
                }
            }
        }

        @Override public void channelInactive(ChannelHandlerContext ctx) {
            if (flushTask != null) {
                flushTask.cancel(false);
            }
            if (retryTask != null) {
                retryTask.cancel(false);
            }
            flushBatch(ctx); // Final flush
            
            // Close persistent TCP connection
            if (tcpConnection != null && tcpConnection.isActive()) {
                System.err.printf("[DEBUG] UDP: Closing persistent TCP connection%n");
                tcpConnection.close();
                tcpConnection = null;
            }
        }

        private void scheduleFlush() {
            if (batchConfig.enable_batching && batchConfig.flush_interval_ms > 0) {
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
            
            System.err.printf("[DEBUG] UDP sendBatchWithSplit: Starting with %d records%n", records.size());
            
            ByteBufAllocator alloc = ctx.alloc();
            ByteBuf batchData = alloc.buffer();
            try {
                // Log total size of records
                long totalSize = records.stream().mapToLong(pkt -> pkt.content().readableBytes()).sum();
                System.err.printf("[DEBUG] UDP sendBatchWithSplit: Total records size: %d bytes%n", totalSize);
                
                for (DatagramPacket pkt : records) {
                    batchData.writeBytes(pkt.content(), pkt.content().readerIndex(), pkt.content().readableBytes());
                }
                
                System.err.printf("[DEBUG] UDP sendBatchWithSplit: Combined batch size: %d bytes%n", batchData.readableBytes());
                
                ByteBuf compressedData = batchData;
                if (batchConfig.enable_compression && batchData.readableBytes() > 0) {
                    System.err.printf("[DEBUG] UDP sendBatchWithSplit: Compressing data...%n");
                    compressedData = compressData(batchData, alloc);
                    batchData.release();
                    System.err.printf("[DEBUG] UDP sendBatchWithSplit: Compressed size: %d bytes (compression ratio: %.2f%%)%n", 
                        compressedData.readableBytes(), 
                        (100.0 * compressedData.readableBytes() / totalSize));
                } else {
                    System.err.printf("[DEBUG] UDP sendBatchWithSplit: Compression disabled or empty data%n");
                }
                
                System.err.printf("[DEBUG] UDP sendBatchWithSplit: Sending %d records, %d bytes%n", 
                    records.size(), compressedData.readableBytes());
                
                sendBatchViaTcp(ctx, compressedData, records);
            } catch (Throwable t) {
                System.err.printf("[DEBUG] UDP sendBatchWithSplit: Exception during processing: %s%n", t.getMessage());
                batchData.release();
                for (DatagramPacket pkt : records) pkt.content().release();
                System.err.printf("Error in sendBatchWithSplit (UDP): %s%n", t.getMessage());
            }
        }

        private void sendBatchViaTcp(ChannelHandlerContext ctx, ByteBuf data, java.util.List<DatagramPacket> records) {
            // Check if we should use UDP forwarding
            if ("udp".equalsIgnoreCase(route.forward.protocol)) {
                sendBatchViaUdp(ctx, data, records);
                return;
            }
            
            final int recordCount = records.size();
            
            if (tcpConnection != null && tcpConnection.isActive()) {
                // Use existing persistent connection
                tcpConnection.writeAndFlush(data).addListener(future -> {
                    if (!future.isSuccess()) {
                        System.err.printf("[DEBUG] UDP sendBatchViaTcp: Send failed - %s%n", future.cause().getMessage());
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
                        System.err.printf("[DEBUG] UDP sendBatchViaTcp: Successfully sent %d records via persistent connection%n", recordCount);
                        for (DatagramPacket pkt : records) pkt.content().release();
                    }
                });
            } else {
                // No persistent connection available, queue for retry
                System.err.printf("[DEBUG] UDP sendBatchViaTcp: No persistent connection, queuing batch%n");
                for (DatagramPacket pkt : records) {
                    retryQueue.offer(new DatagramPacket(pkt.content().retain(), null));
                }
                data.release();
                
                // Try to establish connection if not already connecting
                if (!isConnecting) {
                    establishTcpConnection();
                }
            }
        }

        private void sendBatchViaUdp(ChannelHandlerContext ctx, ByteBuf data, java.util.List<DatagramPacket> records) {
            final int recordCount = records.size();
            System.err.printf("[DEBUG] UDP sendBatchViaUdp: Sending %d records, %d bytes via UDP to %s:%d%n", 
                recordCount, data.readableBytes(), route.forward.host, route.forward.port);
            
            // Create UDP channel for sending
            Bootstrap b = new Bootstrap()
                    .group(ctx.channel().eventLoop())
                    .channel(NioDatagramChannel.class);
            
            b.connect(route.forward.host, route.forward.port).addListener((ChannelFuture f) -> {
                if (f.isSuccess()) {
                    DatagramPacket packet = new DatagramPacket(data, 
                        new InetSocketAddress(route.forward.host, route.forward.port));
                    
                    f.channel().writeAndFlush(packet).addListener(future -> {
                        if (!future.isSuccess()) {
                            System.err.printf("[DEBUG] UDP sendBatchViaUdp: Send failed - %s%n", future.cause().getMessage());
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
                            System.err.printf("[DEBUG] UDP sendBatchViaUdp: Successfully sent %d records via UDP%n", recordCount);
                            for (DatagramPacket pkt : records) pkt.content().release();
                        }
                        f.channel().close();
                    });
                } else {
                    System.err.printf("[DEBUG] UDP sendBatchViaUdp: Connection failed - %s%n", f.cause().getMessage());
                    data.release();
                    for (DatagramPacket pkt : records) pkt.content().release();
                }
            });
        }

        private ByteBuf compressData(ByteBuf data, ByteBufAllocator alloc) throws IOException {
            if (!batchConfig.enable_compression) {
                return data.retain(); // Return uncompressed data
            }
            
            // Snappy compression
            byte[] input = new byte[data.readableBytes()];
            data.getBytes(data.readerIndex(), input);
            byte[] compressed = Snappy.compress(input);
            ByteBuf result = alloc.buffer(compressed.length);
            result.writeBytes(compressed);
            return result;
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