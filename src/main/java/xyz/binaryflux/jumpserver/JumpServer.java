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
import io.netty.channel.socket.DatagramPacket;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    private static final Logger logger = LogManager.getLogger(JumpServer.class);

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
                logger.error("[{}] Unknown protocol {}", r.name, r.listen.protocol);
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
                logger.debug("Already attempting to reconnect, skipping");
                return;
            }
            
            isReconnecting = true;
            logger.debug("Attempting to connect to {}:{}", route.forward.host, route.forward.port);
            
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
                    logger.debug("Successfully connected to backend");
                    isReconnecting = false;
                    outbound = f.channel();
                    
                    // Process any pending messages
                    processPendingMessages();
                    
                    // Add connection close listener
                    outbound.closeFuture().addListener((ChannelFuture closeFuture) -> {
                        logger.debug("Backend connection closed");
                        outbound = null;
                        
                        // Clean up any remaining pending messages
                        cleanupPendingMessages();
                        
                        if (batchConfig.persistent_reconnection && clientCtx != null && clientCtx.channel().isActive()) {
                            scheduleReconnect();
                        }
                    });
                    
                    ctx.channel().read();
                } else {
                    logger.error("Connection failed: {}", f.cause().getMessage());
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
            
            logger.debug("Processing {} pending messages", pendingMessages.size());
            
            ByteBuf msg;
            while ((msg = pendingMessages.poll()) != null) {
                if (outbound != null && outbound.isActive()) {
                    if (!batchConfig.enable_batching) {
                        // Send immediately when batching is disabled
                        final ByteBuf finalMsg = msg;
                        outbound.writeAndFlush(msg.retain()).addListener((ChannelFuture future) -> {
                            if (!future.isSuccess()) {
                                logger.error("Failed to send pending message: {}", future.cause().getMessage());
                            } else {
                                logger.debug("Successfully sent pending message");
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
                logger.debug("Cancelled cleanup task - all messages processed");
            }
        }
        
        private void cleanupPendingMessages() {
            logger.debug("Cleaning up {} pending messages", pendingMessages.size());
            ByteBuf msg;
            while ((msg = pendingMessages.poll()) != null) {
                msg.release();
            }
        }
        
        private void scheduleReconnect() {
            if (reconnectTask != null && !reconnectTask.isDone()) {
                return; // Already scheduled
            }
            
            logger.debug("Scheduling reconnection in {} ms", batchConfig.connection_retry_delay_ms);
            reconnectTask = clientCtx.executor().schedule(
                () -> connect(clientCtx),
                batchConfig.connection_retry_delay_ms,
                TimeUnit.MILLISECONDS
            );
        }

        @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
            logger.debug("Msg received: {}", msg);
            if (outbound != null && outbound.isActive()) {
                // Pass the message to the batched forwarder
                outbound.pipeline().get(BatchedForwarder.class).addToBatch(msg);
            } else {
                logger.error("Backend not connected, queuing message for retry");
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
            logger.info("Client disconnected: {}", ctx.channel().remoteAddress());
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
        private final Channel clientChannel;  // Channel to client (for responses)
        private final BatchConfig batchConfig;
        private final ConcurrentLinkedQueue<ByteBuf> batchQueue = new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<ByteBuf> retryQueue = new ConcurrentLinkedQueue<>();
        private int currentBatchSize = 0;
        private int currentBatchBytes = 0;
        private ScheduledFuture<?> flushTask;
        private ScheduledFuture<?> retryTask;
        private ChannelHandlerContext backendCtx;  // Context for backend connection

        BatchedForwarder(Channel clientChannel, BatchConfig batchConfig) {
            this.clientChannel = clientChannel;
            this.batchConfig = batchConfig;
            if (batchConfig.enable_batching) {
                scheduleFlush();
            }
            scheduleRetryTask();
        }

        @Override public void channelActive(ChannelHandlerContext ctx) {
            this.backendCtx = ctx;
        }

        @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (!batchConfig.enable_batching) {
                // Send immediately, no compression
                if (msg instanceof ByteBuf) {
                    clientChannel.writeAndFlush(msg);
                }
                return;
            }
            // Forward response data back to the client channel
            clientChannel.writeAndFlush(msg);
        }

        @Override public void channelInactive(ChannelHandlerContext ctx) {
            if (flushTask != null) {
                flushTask.cancel(false);
            }
            if (retryTask != null) {
                retryTask.cancel(false);
            }
            flushBatch(); // Final flush
            clientChannel.close();
        }

        public void queueForRetry(Object msg) {
            if (msg instanceof ByteBuf) {
                ByteBuf buf = (ByteBuf) msg;
                logger.debug("queueForRetry: Queuing {} bytes for retry", buf.readableBytes());
                retryQueue.offer(buf.retain());
            }
        }

        private void scheduleRetryTask() {
            if (batchConfig.send_retry_delay_ms > 0) {
                retryTask = clientChannel.eventLoop().scheduleAtFixedRate(
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
            
            logger.debug("processRetryQueue: Processing {} retry items", retryQueue.size());
            
            ByteBuf msg;
            while ((msg = retryQueue.poll()) != null) {
                addToBatch(msg);
            }
        }

        public void addToBatch(Object msg) {
            if (msg instanceof ByteBuf) {
                ByteBuf buf = (ByteBuf) msg;
                int msgSize = buf.readableBytes();
                
                logger.debug("addToBatch: Received message of {} bytes, current batch: {} records, {} bytes", 
                    msgSize, currentBatchSize, currentBatchBytes);
                
                if (!batchConfig.enable_batching) {
                    // Send immediately when batching is disabled with retry logic
                    logger.debug("addToBatch: Batching disabled, sending immediately");
                    sendWithRetry(buf, 0);
                    return;
                }
                
                // Check if adding this message would exceed batch limits
                if (currentBatchSize >= batchConfig.max_batch_size || 
                    currentBatchBytes + msgSize >= batchConfig.max_batch_bytes) {
                    logger.debug("addToBatch: Batch limits reached, flushing batch");
                    flushBatch();
                }
                
                // Add to batch
                batchQueue.offer(buf.retain()); // Retain to keep reference
                currentBatchSize++;
                currentBatchBytes += msgSize;
                
                logger.debug("addToBatch: Added to batch, new size: {} records, {} bytes", 
                    currentBatchSize, currentBatchBytes);
            }
        }

        private void sendWithRetry(ByteBuf buf, int attempt) {
            logger.debug("sendWithRetry: Attempting to send {} bytes to backend (attempt {})", 
                buf.readableBytes(), attempt + 1);
            
            // Use the backend context for sending
            if (backendCtx != null && backendCtx.channel().isActive()) {
                backendCtx.writeAndFlush(buf.retain()).addListener((ChannelFuture future) -> {
                    if (!future.isSuccess()) {
                        logger.error("sendWithRetry: Send failed (attempt {}) - {}", 
                            attempt + 1, future.cause().getMessage());
                        
                        if (attempt < batchConfig.max_send_retries) {
                            logger.debug("sendWithRetry: Retrying in {} ms", batchConfig.send_retry_delay_ms);
                            clientChannel.eventLoop().schedule(
                                () -> sendWithRetry(buf, attempt + 1),
                                batchConfig.send_retry_delay_ms,
                                TimeUnit.MILLISECONDS
                            );
                        } else {
                            logger.debug("sendWithRetry: Max retries reached, queuing for later retry");
                            retryQueue.offer(buf.retain());
                            buf.release();
                        }
                    } else {
                        logger.debug("sendWithRetry: Successfully sent {} bytes to backend (attempt {})", 
                            buf.readableBytes(), attempt + 1);
                        buf.release();
                    }
                });
            } else {
                logger.debug("sendWithRetry: Backend channel not active, queuing for retry");
                retryQueue.offer(buf.retain());
                buf.release();
            }
        }

        private void scheduleFlush() {
            if (batchConfig.enable_batching && batchConfig.flush_interval_ms > 0) {
                flushTask = clientChannel.eventLoop().scheduleAtFixedRate(
                    this::flushBatch,
                    batchConfig.flush_interval_ms,
                    batchConfig.flush_interval_ms,
                    TimeUnit.MILLISECONDS
                );
            }
        }

        private void flushBatch() {
            if (batchQueue.isEmpty()) {
                logger.debug("flushBatch: Queue is empty, nothing to flush");
                return;
            }
            
            logger.debug("flushBatch: Starting flush of {} records, {} bytes", 
                currentBatchSize, currentBatchBytes);
            
            try {
                // Gather all records for splitting if needed
                java.util.List<ByteBuf> records = new java.util.ArrayList<>();
                ByteBuf msg;
                while ((msg = batchQueue.poll()) != null) {
                    records.add(msg);
                }
                
                logger.debug("flushBatch: Gathered {} records for sending", records.size());
                sendBatchWithSplit(records);
                
                // Reset batch counters
                currentBatchSize = 0;
                currentBatchBytes = 0;
                logger.debug("flushBatch: Reset batch counters");
            } catch (Exception e) {
                logger.error("flushBatch: Exception during flush: {}", e.getMessage());
                logger.error("Error flushing batch: {}", e.getMessage());
            }
        }

        private void sendBatchWithSplit(java.util.List<ByteBuf> records) {
            if (records.isEmpty()) return;
            
            logger.debug("sendBatchWithSplit: Starting with {} records", records.size());
            
            ByteBufAllocator alloc = clientChannel.alloc();
            ByteBuf batchData = alloc.buffer();
            try {
                // Log total size of records
                long totalSize = records.stream().mapToLong(ByteBuf::readableBytes).sum();
                logger.debug("sendBatchWithSplit: Total records size: {} bytes", totalSize);
                
                for (ByteBuf buf : records) {
                    batchData.writeBytes(buf, buf.readerIndex(), buf.readableBytes());
                }
                
                logger.debug("sendBatchWithSplit: Combined batch size: {} bytes", batchData.readableBytes());
                
                ByteBuf compressedData = batchData;
                if (batchConfig.enable_compression && batchData.readableBytes() > 0) {
                    logger.debug("sendBatchWithSplit: Compressing data...");
                    compressedData = compressData(batchData, alloc);
                    batchData.release();
                    logger.debug("sendBatchWithSplit: Compressed size: {} bytes (compression ratio: {:.2f}%)", 
                        compressedData.readableBytes(), 
                        (100.0 * compressedData.readableBytes() / totalSize));
                } else {
                    logger.debug("sendBatchWithSplit: Compression disabled or empty data");
                }
                
                final int recordCount = records.size();
                final ByteBuf finalCompressedData = compressedData;
                final int bytesToSend = finalCompressedData.readableBytes();
                
                logger.debug("sendBatchWithSplit: Sending {} records, {} bytes", 
                    recordCount, bytesToSend);
                
                // Debug: Show first few bytes of data being sent
                if (bytesToSend > 0) {
                    byte[] debugBytes = new byte[Math.min(32, bytesToSend)];
                    finalCompressedData.getBytes(finalCompressedData.readerIndex(), debugBytes);
                    StringBuilder hex = new StringBuilder();
                    for (byte b : debugBytes) {
                        hex.append(String.format("%02x ", b));
                    }
                    logger.debug("sendBatchWithSplit: First {} bytes (hex): {}", debugBytes.length, hex.toString());
                }
                
                // Use the backend context for sending
                if (backendCtx != null && backendCtx.channel().isActive()) {
                    logger.debug("sendBatchWithSplit: Backend channel is active, sending data");
                    backendCtx.writeAndFlush(finalCompressedData).addListener((ChannelFuture future) -> {
                        if (!future.isSuccess()) {
                            Throwable cause = future.cause();
                            logger.error("sendBatchWithSplit: Send failed - {}", cause.getMessage());
                            
                            boolean isClosed = cause instanceof java.nio.channels.ClosedChannelException ||
                                               (cause != null && cause.getClass().getSimpleName().contains("ClosedChannelException"));
                            
                            if (recordCount > 1 && !isClosed) {
                                logger.debug("sendBatchWithSplit: Splitting batch of {} records and retrying", recordCount);
                                int mid = recordCount / 2;
                                java.util.List<ByteBuf> left = records.subList(0, mid);
                                java.util.List<ByteBuf> right = records.subList(mid, recordCount);
                                sendBatchWithSplit(new java.util.ArrayList<>(left));
                                sendBatchWithSplit(new java.util.ArrayList<>(right));
                            } else {
                                if (isClosed) {
                                    logger.debug("sendBatchWithSplit: Channel closed, writing {} records to retry file", recordCount);
                                    RetryFileUtil.writeFailedBatch(records, batchConfig.enable_compression);
                                } else {
                                    logger.debug("sendBatchWithSplit: Single record failed, logging error");
                                }
                                for (ByteBuf buf : records) buf.release();
                            }
                        } else {
                            logger.debug("sendBatchWithSplit: Successfully sent {} records, {} bytes to backend", 
                                recordCount, bytesToSend);
                            logger.debug("sendBatchWithSplit: Backend channel writable: {}, active: {}", 
                                backendCtx.channel().isWritable(), backendCtx.channel().isActive());
                            for (ByteBuf buf : records) buf.release();
                        }
                    });
                } else {
                    logger.debug("sendBatchWithSplit: Backend channel not active, writing to retry file");
                    if (backendCtx == null) {
                        logger.debug("sendBatchWithSplit: Backend context is null");
                    } else {
                        logger.debug("sendBatchWithSplit: Backend channel active: {}, writable: {}", 
                            backendCtx.channel().isActive(), backendCtx.channel().isWritable());
                    }
                    RetryFileUtil.writeFailedBatch(records, batchConfig.enable_compression);
                    for (ByteBuf buf : records) buf.release();
                }
            } catch (Throwable t) {
                logger.error("sendBatchWithSplit: Exception during processing: {}", t.getMessage());
                batchData.release();
                for (ByteBuf buf : records) buf.release();
                logger.error("Error in sendBatchWithSplit: {}", t.getMessage());
            }
        }

        private ByteBuf compressData(ByteBuf data, ByteBufAllocator alloc) throws IOException {
            if (!batchConfig.enable_compression) {
                return data.retain(); // Return uncompressed data
            }
            
            logger.debug("compressData: Starting compression of {} bytes", data.readableBytes());
            
            // Log first few bytes of input data
            if (data.readableBytes() > 0) {
                byte[] debugInput = new byte[Math.min(32, data.readableBytes())];
                data.getBytes(data.readerIndex(), debugInput);
                StringBuilder hexInput = new StringBuilder();
                for (byte b : debugInput) {
                    hexInput.append(String.format("%02x ", b));
                }
                logger.debug("compressData: Input data first {} bytes (hex): {}", debugInput.length, hexInput.toString());
                
                // Also show as string if possible
                String inputString = new String(debugInput, java.nio.charset.StandardCharsets.UTF_8);
                logger.debug("compressData: Input data first {} bytes (string): {}", debugInput.length, inputString);
            }
            
            // Snappy compression
            byte[] input = new byte[data.readableBytes()];
            data.getBytes(data.readerIndex(), input);
            
            logger.debug("compressData: Calling Snappy.compress() with {} bytes", input.length);
            byte[] compressed = Snappy.compress(input);
            logger.debug("compressData: Snappy.compress() returned {} bytes", compressed.length);
            
            // Log first few bytes of compressed data
            if (compressed.length > 0) {
                byte[] debugCompressed = new byte[Math.min(32, compressed.length)];
                System.arraycopy(compressed, 0, debugCompressed, 0, debugCompressed.length);
                StringBuilder hexCompressed = new StringBuilder();
                for (byte b : debugCompressed) {
                    hexCompressed.append(String.format("%02x ", b));
                }
                logger.debug("compressData: Compressed data first {} bytes (hex): {}", debugCompressed.length, hexCompressed.toString());
            }
            
            ByteBuf result = alloc.buffer(compressed.length);
            result.writeBytes(compressed);
            
            logger.debug("compressData: Created ByteBuf with {} bytes, compression ratio: {:.2f}%", 
                result.readableBytes(), (100.0 * result.readableBytes() / data.readableBytes()));
            
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
                logger.debug("UDP: Using UDP forwarding to {}:{}", 
                    route.forward.host, route.forward.port);
                return; // No TCP connection needed for UDP forwarding
            }
            
            isConnecting = true;
            logger.debug("UDP: Establishing persistent TCP connection to {}:{}", 
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
                    logger.debug("UDP: Persistent TCP connection established");
                    
                    // Add connection close listener
                    tcpConnection.closeFuture().addListener((ChannelFuture closeFuture) -> {
                        logger.debug("UDP: TCP connection closed, will reconnect");
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
                    logger.error("UDP: TCP connection failed: {}", f.cause().getMessage());
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
                        logger.error("UDP sendSingleViaTcpWithRetry: Send failed (attempt {}) - {}", 
                            attempt + 1, writeFuture.cause().getMessage());
                        
                        if (attempt < batchConfig.max_send_retries) {
                            logger.debug("UDP sendSingleViaTcpWithRetry: Retrying in {} ms", batchConfig.send_retry_delay_ms);
                            ctx.executor().schedule(
                                () -> sendSingleViaTcpWithRetry(ctx, data, attempt + 1),
                                batchConfig.send_retry_delay_ms,
                                TimeUnit.MILLISECONDS
                            );
                        } else {
                            logger.debug("UDP sendSingleViaTcpWithRetry: Max retries reached, queuing for later retry");
                            retryQueue.offer(new DatagramPacket(data.retain(), null));
                            data.release();
                        }
                    } else {
                        logger.debug("UDP sendSingleViaTcpWithRetry: Successfully sent {} bytes via persistent connection (attempt {})", 
                            data.readableBytes(), attempt + 1);
                        data.release();
                    }
                });
            } else {
                // No persistent connection available, queue for retry
                logger.debug("UDP sendSingleViaTcpWithRetry: No persistent connection, queuing message");
                retryQueue.offer(new DatagramPacket(data.retain(), null));
                data.release();
                
                // Try to establish connection if not already connecting
                if (!isConnecting) {
                    establishTcpConnection();
                }
            }
        }

        private void sendSingleViaUdpWithRetry(ChannelHandlerContext ctx, ByteBuf data, int attempt) {
            logger.debug("UDP sendSingleViaUdpWithRetry: Sending {} bytes via UDP to {}:{} (attempt {})", 
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
                            logger.error("UDP sendSingleViaUdpWithRetry: Send failed (attempt {}) - {}", 
                                attempt + 1, writeFuture.cause().getMessage());
                            
                            if (attempt < batchConfig.max_send_retries) {
                                logger.debug("UDP sendSingleViaUdpWithRetry: Retrying in {} ms", batchConfig.send_retry_delay_ms);
                                ctx.executor().schedule(
                                    () -> sendSingleViaUdpWithRetry(ctx, data, attempt + 1),
                                    batchConfig.send_retry_delay_ms,
                                    TimeUnit.MILLISECONDS
                                );
                            } else {
                                logger.debug("UDP sendSingleViaUdpWithRetry: Max retries reached, queuing for later retry");
                                retryQueue.offer(new DatagramPacket(data.retain(), null));
                                data.release();
                            }
                        } else {
                            logger.debug("UDP sendSingleViaUdpWithRetry: Successfully sent {} bytes via UDP (attempt {})", 
                                data.readableBytes(), attempt + 1);
                            data.release();
                        }
                        f.channel().close();
                    });
                } else {
                    logger.error("UDP sendSingleViaUdpWithRetry: Connection failed (attempt {}) - {}", 
                        attempt + 1, f.cause().getMessage());
                    
                    if (attempt < batchConfig.max_send_retries) {
                        logger.debug("UDP sendSingleViaUdpWithRetry: Retrying in {} ms", batchConfig.send_retry_delay_ms);
                        ctx.executor().schedule(
                            () -> sendSingleViaUdpWithRetry(ctx, data, attempt + 1),
                            batchConfig.send_retry_delay_ms,
                            TimeUnit.MILLISECONDS
                        );
                    } else {
                        logger.debug("UDP sendSingleViaUdpWithRetry: Max retries reached, queuing for later retry");
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
            
            logger.debug("UDP processRetryQueue: Processing {} retry items", retryQueue.size());
            
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
                logger.debug("UDP: Closing persistent TCP connection");
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
                logger.error("Error flushing UDP batch: {}", e.getMessage());
            }
        }

        private void sendBatchWithSplit(ChannelHandlerContext ctx, java.util.List<DatagramPacket> records) {
            if (records.isEmpty()) return;
            
            logger.debug("UDP sendBatchWithSplit: Starting with {} records", records.size());
            
            ByteBufAllocator alloc = ctx.alloc();
            ByteBuf batchData = alloc.buffer();
            try {
                // Log total size of records
                long totalSize = records.stream().mapToLong(pkt -> pkt.content().readableBytes()).sum();
                logger.debug("UDP sendBatchWithSplit: Total records size: {} bytes", totalSize);
                
                for (DatagramPacket pkt : records) {
                    batchData.writeBytes(pkt.content(), pkt.content().readerIndex(), pkt.content().readableBytes());
                }
                
                logger.debug("UDP sendBatchWithSplit: Combined batch size: {} bytes", batchData.readableBytes());
                
                ByteBuf compressedData = batchData;
                if (batchConfig.enable_compression && batchData.readableBytes() > 0) {
                    logger.debug("UDP sendBatchWithSplit: Compressing data...");
                    compressedData = compressData(batchData, alloc);
                    batchData.release();
                    logger.debug("UDP sendBatchWithSplit: Compressed size: {} bytes (compression ratio: {:.2f}%)", 
                        compressedData.readableBytes(), 
                        (100.0 * compressedData.readableBytes() / totalSize));
                } else {
                    logger.debug("UDP sendBatchWithSplit: Compression disabled or empty data");
                }
                
                logger.debug("UDP sendBatchWithSplit: Sending {} records, {} bytes", 
                    records.size(), compressedData.readableBytes());
                
                sendBatchViaTcp(ctx, compressedData, records);
            } catch (Throwable t) {
                logger.error("UDP sendBatchWithSplit: Exception during processing: {}", t.getMessage());
                batchData.release();
                for (DatagramPacket pkt : records) pkt.content().release();
                logger.error("Error in sendBatchWithSplit (UDP): {}", t.getMessage());
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
                        logger.error("UDP sendBatchViaTcp: Send failed - {}", future.cause().getMessage());
                        if (recordCount > 1) {
                            int mid = recordCount / 2;
                            java.util.List<DatagramPacket> left = records.subList(0, mid);
                            java.util.List<DatagramPacket> right = records.subList(mid, recordCount);
                            sendBatchWithSplit(ctx, new java.util.ArrayList<>(left));
                            sendBatchWithSplit(ctx, new java.util.ArrayList<>(right));
                        } else {
                            logger.error("Failed to send single UDP record: {}", future.cause());
                            records.get(0).content().release();
                        }
                    } else {
                        logger.debug("UDP sendBatchViaTcp: Successfully sent {} records via persistent connection", recordCount);
                        for (DatagramPacket pkt : records) pkt.content().release();
                    }
                });
            } else {
                // No persistent connection available, queue for retry
                logger.debug("UDP sendBatchViaTcp: No persistent connection, queuing batch");
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
            logger.debug("UDP sendBatchViaUdp: Sending {} records, {} bytes via UDP to {}:{}", 
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
                            logger.error("UDP sendBatchViaUdp: Send failed - {}", future.cause().getMessage());
                            if (recordCount > 1) {
                                int mid = recordCount / 2;
                                java.util.List<DatagramPacket> left = records.subList(0, mid);
                                java.util.List<DatagramPacket> right = records.subList(mid, recordCount);
                                sendBatchWithSplit(ctx, new java.util.ArrayList<>(left));
                                sendBatchWithSplit(ctx, new java.util.ArrayList<>(right));
                            } else {
                                logger.error("Failed to send single UDP record: {}", future.cause());
                                records.get(0).content().release();
                            }
                        } else {
                            logger.debug("UDP sendBatchViaUdp: Successfully sent {} records via UDP", recordCount);
                            for (DatagramPacket pkt : records) pkt.content().release();
                        }
                        f.channel().close();
                    });
                } else {
                    logger.error("UDP sendBatchViaUdp: Connection failed - {}", f.cause().getMessage());
                    data.release();
                    for (DatagramPacket pkt : records) pkt.content().release();
                }
            });
        }

        private ByteBuf compressData(ByteBuf data, ByteBufAllocator alloc) throws IOException {
            if (!batchConfig.enable_compression) {
                return data.retain(); // Return uncompressed data
            }
            
            logger.debug("compressData: Starting compression of {} bytes", data.readableBytes());
            
            // Log first few bytes of input data
            if (data.readableBytes() > 0) {
                byte[] debugInput = new byte[Math.min(32, data.readableBytes())];
                data.getBytes(data.readerIndex(), debugInput);
                StringBuilder hexInput = new StringBuilder();
                for (byte b : debugInput) {
                    hexInput.append(String.format("%02x ", b));
                }
                logger.debug("compressData: Input data first {} bytes (hex): {}", debugInput.length, hexInput.toString());
                
                // Also show as string if possible
                String inputString = new String(debugInput, java.nio.charset.StandardCharsets.UTF_8);
                logger.debug("compressData: Input data first {} bytes (string): {}", debugInput.length, inputString);
            }
            
            // Snappy compression
            byte[] input = new byte[data.readableBytes()];
            data.getBytes(data.readerIndex(), input);
            
            logger.debug("compressData: Calling Snappy.compress() with {} bytes", input.length);
            byte[] compressed = Snappy.compress(input);
            logger.debug("compressData: Snappy.compress() returned {} bytes", compressed.length);
            
            // Log first few bytes of compressed data
            if (compressed.length > 0) {
                byte[] debugCompressed = new byte[Math.min(32, compressed.length)];
                System.arraycopy(compressed, 0, debugCompressed, 0, debugCompressed.length);
                StringBuilder hexCompressed = new StringBuilder();
                for (byte b : debugCompressed) {
                    hexCompressed.append(String.format("%02x ", b));
                }
                logger.debug("compressData: Compressed data first {} bytes (hex): {}", debugCompressed.length, hexCompressed.toString());
            }
            
            ByteBuf result = alloc.buffer(compressed.length);
            result.writeBytes(compressed);
            
            logger.debug("compressData: Created ByteBuf with {} bytes, compression ratio: {:.2f}%", 
                result.readableBytes(), (100.0 * result.readableBytes() / data.readableBytes()));
            
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
            logger.info("[{}] listening {} / {} ({})",
                    r.name, r.listen.ip, r.listen.port, r.listen.protocol);
        else
            logger.error("[{}] bind failed: {}", r.name, f.cause());
    }
}