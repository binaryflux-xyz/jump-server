package xyz.binaryflux.jumpserver;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.yaml.snakeyaml.Yaml;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
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
                startTcpRoute(r, sslCtx, bossGroup, workerGroup);
            } else if ("udp".equalsIgnoreCase(r.listen.protocol)) {
                startUdpRoute(r, sslCtx, workerGroup);
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
                                      EventLoopGroup boss, EventLoopGroup workers) {

        new ServerBootstrap()
                .group(boss, workers)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new TcpRelayHandler(r, sslCtx));
                    }
                })
                .bind(new InetSocketAddress(r.listen.ip, r.listen.port))
                .addListener((ChannelFuture f) -> logBind(r, f));
    }

    private static final class TcpRelayHandler extends ChannelInboundHandlerAdapter {
        private final Route route;
        private final SslContext sslCtx;
        private Channel outbound;

        private static final int MAX_RETRIES = 5;
        private static final long RETRY_DELAY_MS = 2000;
        private int attempts = 0;

        TcpRelayHandler(Route route, SslContext sslCtx) { this.route = route; this.sslCtx = sslCtx; }

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
                            ch.pipeline().addLast(new Forwarder(ctx.channel()));
                        }
                    });
            b.connect(route.forward.host, route.forward.port).addListener((ChannelFuture f) -> {
                if (f.isSuccess()) outbound = f.channel();
                else ctx.close();
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
            if (outbound != null) outbound.writeAndFlush(msg);
        }

        @Override public void channelInactive(ChannelHandlerContext ctx) {
            if (outbound != null) outbound.close();
        }

        private boolean useTls() { return route.forward.tls == null || route.forward.tls; }
    }

    private static final class Forwarder extends ChannelInboundHandlerAdapter {
        private final Channel inbound;
        Forwarder(Channel inbound) { this.inbound = inbound; }
        @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
            inbound.writeAndFlush(msg);
        }
        @Override public void channelInactive(ChannelHandlerContext ctx) { inbound.close(); }
    }

    /* ---------- UDP ---------- */

    private static void startUdpRoute(Route r, SslContext sslCtx, EventLoopGroup group) {

        new Bootstrap().group(group)
                .channel(NioDatagramChannel.class)
                .handler(new SimpleChannelInboundHandler<DatagramPacket>() {
                    @Override protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket pkt) {
                        // fire & forget: open TLS or plain TCP each datagram
                        Bootstrap b = new Bootstrap()
                                .group(ctx.channel().eventLoop())
                                .channel(NioSocketChannel.class)
                                .handler(new ChannelInitializer<SocketChannel>() {
                                    @Override protected void initChannel(SocketChannel ch) {
                                        if (useTls()) ch.pipeline().addLast(
                                                sslCtx.newHandler(ch.alloc(),
                                                        r.forward.host, r.forward.port));
                                    }
                                    private boolean useTls() {
                                        return r.forward.tls == null || r.forward.tls;
                                    }
                                });

                        b.connect(r.forward.host, r.forward.port).addListener((ChannelFuture f) -> {
                            if (f.isSuccess()) {
                                f.channel().writeAndFlush(
                                        Unpooled.copiedBuffer(pkt.content()))
                                        .addListener(ChannelFutureListener.CLOSE);
                            } else {
                                pkt.content().release();
                            }
                        });
                    }
                })
                .bind(new InetSocketAddress(r.listen.ip, r.listen.port))
                .addListener((ChannelFuture f) -> logBind(r, f));
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