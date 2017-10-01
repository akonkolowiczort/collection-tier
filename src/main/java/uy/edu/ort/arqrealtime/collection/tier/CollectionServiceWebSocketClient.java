package uy.edu.ort.arqrealtime.collection.tier;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketClientCompressionHandler;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

public class CollectionServiceWebSocketClient {

    private static final String URL = "ws://stream.meetup.com/2/rsvps";
    private static final int PORT = 80;
    private static final EventLoopGroup group = new NioEventLoopGroup();

    private static boolean continueRunning = true;

    public static void main(String[] args) throws Exception {

        initializeMessageLogger();

        final RSVPProducer rsvpProducer = RSVPProducerFactory.getInstance(args);

        Runtime.getRuntime().addShutdownHook(createShutdownHook(rsvpProducer));

        final URI uri = new URI(URL);

        try {
            final MeetupWebSocketClientHandler handler = createWebSocketClientHandler(uri, rsvpProducer);

            final Bootstrap b = buildBootstrap(handler);

            final Channel ch = b.connect(uri.getHost(), PORT).sync().channel();
            handler.handshakeFuture().sync();
            repeatUntilExit(ch);

        } finally {
            group.shutdownGracefully();
        }
    }

    private static void initializeMessageLogger(){
        try {
            HybridMessageLogger.initialize();
        } catch (Exception exception) {
            System.err.println("Could not initialize HybridMessageLogger!");
            exception.printStackTrace();
            System.exit(-1);
        }
    }

    private static Thread createShutdownHook(final RSVPProducer rsvpProducer) {
        return new Thread(() -> {
            try {
                System.out.println("Shutdown started...");
                group.shutdownGracefully();
                rsvpProducer.close();
                HybridMessageLogger.close();
                continueRunning = false;
                System.out.println("Shutdown finished");
            } catch (final Exception ex) {
                ex.printStackTrace();
            }
        });
    }

    private static MeetupWebSocketClientHandler createWebSocketClientHandler(final URI uri, final RSVPProducer rsvpProducer){
        return new MeetupWebSocketClientHandler(
                WebSocketClientHandshakerFactory.newHandshaker(
                        uri, WebSocketVersion.V13, null, false, new DefaultHttpHeaders()), rsvpProducer);
    }

    private static Bootstrap buildBootstrap(final ChannelHandler channelHandler) {
        Bootstrap b = new Bootstrap();
        b.group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new HttpClientCodec());
                        p.addLast(new HttpObjectAggregator(8192));
                        p.addLast(WebSocketClientCompressionHandler.INSTANCE);
                        p.addLast(channelHandler);
                    }
                });
        return b;
    }

    private static void repeatUntilExit(final Channel ch) throws Exception {
        BufferedReader console = new BufferedReader(new InputStreamReader(System.in));

        do {
            String msg = console.readLine();
            if (msg == null) {
                continueRunning = false;
            } else if ("bye".equals(msg.toLowerCase())) {
                ch.writeAndFlush(new CloseWebSocketFrame());
                ch.closeFuture().sync();
                continueRunning = false;
            } else if ("ping".equals(msg.toLowerCase())) {
                WebSocketFrame frame = new PingWebSocketFrame(Unpooled.wrappedBuffer(new byte[]{8, 1, 8, 1}));
                ch.writeAndFlush(frame);
            } else {
                WebSocketFrame frame = new TextWebSocketFrame(msg);
                ch.writeAndFlush(frame);
            }
        } while (continueRunning);
    }
}
