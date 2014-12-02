package org.mitallast.queue.stomp;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.stomp.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.action.queue.stats.QueueStatsRequest;
import org.mitallast.queue.action.queue.stats.QueueStatsResponse;
import org.mitallast.queue.action.queues.create.CreateQueueRequest;
import org.mitallast.queue.common.BaseQueueTest;
import org.mitallast.queue.common.settings.ImmutableSettings;

import java.util.concurrent.ExecutionException;

public class StompTest extends BaseQueueTest {

    private static String QUEUE = "my_queue";
    private Bootstrap stompBootstrap;
    private Channel stompChannel;

    @Before
    public void setUpClient() throws Exception {
        stompBootstrap = new Bootstrap()
            .channel(NioSocketChannel.class)
            .group(new NioEventLoopGroup())
            .handler(new ChannelInitializer<SocketChannel>() {

                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();
                    pipeline.addLast("decoder", new StompSubframeDecoder());
                    pipeline.addLast("encoder", new StompSubframeEncoder());
                    pipeline.addLast("aggregator", new StompSubframeAggregator(1048576));
                    pipeline.addLast("handler", new SimpleChannelInboundHandler<StompFrame>() {

                        private ClientState state;

                        @Override
                        public void channelActive(ChannelHandlerContext ctx) throws Exception {
                            state = ClientState.AUTHENTICATING;
                            StompFrame connFrame = new DefaultStompFrame(StompCommand.CONNECT);
                            connFrame.headers().set(StompHeaders.ACCEPT_VERSION, "1.2");
                            ctx.writeAndFlush(connFrame);
                        }

                        @Override
                        protected void channelRead0(ChannelHandlerContext ctx, StompFrame frame) throws Exception {
                            String subscrReceiptId = "001";
                            String disconReceiptId = "002";
                            switch (frame.command()) {
                                case CONNECTED:
                                    StompFrame subscribeFrame = new DefaultStompFrame(StompCommand.SUBSCRIBE);
                                    subscribeFrame.headers().set(StompHeaders.DESTINATION, QUEUE);
                                    subscribeFrame.headers().set(StompHeaders.RECEIPT, subscrReceiptId);
                                    subscribeFrame.headers().set(StompHeaders.ID, "1");
                                    System.out.println("connected, sending subscribe frame: " + subscribeFrame);
                                    state = ClientState.AUTHENTICATED;
                                    ctx.writeAndFlush(subscribeFrame);
                                    break;
                                case RECEIPT:
                                    String receiptHeader = frame.headers().get(StompHeaders.RECEIPT_ID);
                                    System.out.println("receipt id: " + receiptHeader);
                                    if (state == ClientState.AUTHENTICATED && receiptHeader.equals(subscrReceiptId)) {
                                        StompFrame msgFrame = new DefaultStompFrame(StompCommand.SEND);
                                        msgFrame.headers().set(StompHeaders.DESTINATION, QUEUE);
                                        msgFrame.content().writeBytes("some payload".getBytes());
                                        System.out.println("subscribed, sending message frame: " + msgFrame);
                                        state = ClientState.SUBSCRIBED;
                                        ctx.writeAndFlush(msgFrame);
                                    } else if (state == ClientState.DISCONNECTING && receiptHeader.equals(disconReceiptId)) {
                                        System.out.println("disconnected");
                                        ctx.close();
                                    } else {
                                        throw new IllegalStateException("received: " + frame + ", while internal state is " + state);
                                    }
                                    break;
                                case MESSAGE:
                                    if (state == ClientState.SUBSCRIBED) {
                                        System.out.println("received frame: " + frame);
                                        StompFrame disconnFrame = new DefaultStompFrame(StompCommand.DISCONNECT);
                                        disconnFrame.headers().set(StompHeaders.RECEIPT, disconReceiptId);
                                        System.out.println("sending disconnect frame: " + disconnFrame);
                                        state = ClientState.DISCONNECTING;
                                        ctx.writeAndFlush(disconnFrame);
                                    }
                                    break;
                                default:
                                    break;
                            }
                        }
                    });
                }

                @Override
                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                    cause.printStackTrace();
                    ctx.close();
                }
            });

        stompChannel = stompBootstrap.connect("127.0.0.1", 9080).sync().channel();
    }

    @After
    public void tearDownClient() throws Exception {
        stompChannel.closeFuture().sync();
        stompBootstrap.group().shutdownGracefully();
    }

    @Test
    public void test() throws ExecutionException {
        client().queues().createQueue(new CreateQueueRequest(QUEUE, ImmutableSettings.builder().build())).get();
        QueueStatsResponse response = client().queue().queueStatsRequest(new QueueStatsRequest(QUEUE)).get();
        assert response.getStats().getSize() == 0;

        StompFrame request = new DefaultStompFrame(StompCommand.SEND);
        request.headers().set(StompHeaders.DESTINATION, QUEUE);
        request.headers().set(StompHeaders.CONTENT_TYPE, "text");
        request.headers().set(StompHeaders.RECEIPT, "001");
        request.content().writeBytes("Hello world".getBytes());
        stompChannel.write(request);
        stompChannel.flush();
    }

    private enum ClientState {
        AUTHENTICATING,
        AUTHENTICATED,
        SUBSCRIBED,
        DISCONNECTING
    }
}
