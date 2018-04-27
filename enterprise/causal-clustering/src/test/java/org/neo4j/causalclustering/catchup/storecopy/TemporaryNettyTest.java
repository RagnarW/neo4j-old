/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.DefaultByteBufHolder;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.concurrent.Future;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.replication.DistributedOperation;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.core.replication.session.GlobalSession;
import org.neo4j.causalclustering.core.replication.session.LocalOperationId;
import org.neo4j.causalclustering.core.state.machines.dummy.DummyRequest;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionChunk;
import org.neo4j.causalclustering.helper.StatUtil;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.CoreReplicatedContentMarshal;
import org.neo4j.causalclustering.messaging.marshalling.RaftMessageCompleteDecoder;
import org.neo4j.causalclustering.messaging.marshalling.RaftMessageCompleteEncoder;
import org.neo4j.causalclustering.messaging.marshalling.decoding.RaftMessageComposingDecoder;
import org.neo4j.concurrent.Futures;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.ports.allocation.PortAuthority;

import static org.junit.Assert.assertEquals;

public class TemporaryNettyTest
{
    @Test
    public void name() throws Exception
    {
        int port = PortAuthority.allocatePort();
        TimeStamps timeStamps = new TimeStamps();
//        DummyTransactionHandler dummyTransactionHandler = new DummyTransactionHandler( timeStamps );
        RaftMessageComposingDecoder composingDecoder = new RaftMessageComposingDecoder();
        RaftMessageSimpleChannelInboundHandler raftMessageHandler = new RaftMessageSimpleChannelInboundHandler();
        ChunkedInputHanlder channelInboundHandlerAdapter = new ChunkedInputHanlder( timeStamps );
//        ReTransactionChunkHandler reTransactionChunkHandler = new ReTransactionChunkHandler();
//        ReTransactionChunkHandlerBB reTransactionChunkHandlerBB = new ReTransactionChunkHandlerBB();

        NioEventLoopGroup clientExecutor = new NioEventLoopGroup( 0, new NamedThreadFactory( "test" ) );
        NioEventLoopGroup serverExecutor = new NioEventLoopGroup( 0, new NamedThreadFactory( "test" ) );

        try
        {

            SocketAddress socketAddress = new InetSocketAddress( port );
            ServerBootstrap server = createServer( raftMessageHandler, composingDecoder, serverExecutor );
            server.bind( socketAddress ).awaitUninterruptibly();
            Bootstrap client = createClient( clientExecutor );
            ChannelFuture future = client.connect( socketAddress ).awaitUninterruptibly();
            Channel channel = future.channel();
            StatUtil.StatContext hej = StatUtil.create( "hej", 1000000, true );
            int muliplicationFactor = 1;
            System.currentTimeMillis();
            int loops = 2;
            RaftMessages.RaftMessage message1 = null;
            RaftMessages.RaftMessage message2 = null;
            RaftMessages.RaftMessage message3 = null;
            InMemoryClosableChannel inMemoryClosableChannel = new InMemoryClosableChannel();
            for ( int j = 324; j <= 400; j *= 10 )
            {
                StatUtil.TimingContext time1 = hej.time();
//                StatUtil.TimingContext total = hej.time();
                long start = System.currentTimeMillis();
                byte[] bytes = new byte[j * 1 * muliplicationFactor];
                ThreadLocalRandom.current().nextBytes( bytes );

                for ( int i = 0; i < loops; i++ )
                {
//                    DistributedOperation distributedOperation = createDistributedOperation( bytes );
                    DummyRequest dummyRequest = new DummyRequest( bytes );
                    message1 = composeClusterAware( createNewEntryRequest( dummyRequest ) );
//                    message2 = composeClusterAware( createAppendRequestEntry( createDistributedOperation( bytes ), createDistributedOperation( bytes ) ) );
//                    message3 = composeClusterAware( createNewEntryRequest( createDistributedOperation( bytes ) ) );
//                    raftMessageHandler.setExpected( message );

                    int counter = 0;
                    while ( !channel.isWritable() )
                    {
                        counter++;
                        Thread.sleep( 1 );
                    }
//                    if ( counter > 0 )
//                    {
//                        System.out.println( "counter = " + counter );
//                    }
//                        StatUtil.TimingContext time = hej.time();
                    timeStamps.initWrite();
//                    ChunkedStream msg = new ChunkedStream( new ByteArrayInputStream( bytes ), 8192 *2 );
//                    DummyTransaction dummyTransaction = new DummyTransaction( bytes );

                    channel.writeAndFlush( message1 ).addListener( future1 ->
                    {
//                            time.end();
                        timeStamps.writeSent();
//                            replicatedTransaction.close();
                    } );
//                    channel.writeAndFlush( message2 );
//                    channel.writeAndFlush( message3 ).awaitUninterruptibly();
                }
                long timeMillis = System.currentTimeMillis();
                long l = (long) j * loops;
//                while ( channelInboundHandlerAdapter.counter != l )
//                {
//                    Thread.sleep( 5 );
//                    if ( System.currentTimeMillis() - timeMillis > 12_000 )
//                    {
//                        System.out.println( channelInboundHandlerAdapter.counter + " did not reach " + l );
//                        break;
//                    }
//                }
//                channelInboundHandlerAdapter.clear();
                while ( raftMessageHandler.getCount() != loops )
                {
                    Thread.sleep( 1 );
                    if ( System.currentTimeMillis() - timeMillis > 80_000 )
                    {
                        System.out.println( raftMessageHandler.getCount() + " did not reach " + loops );
                        break;
                    }
                }
                raftMessageHandler.reset();
//                reTransactionChunkHandler.reset();
//                channelInboundHandlerAdapter.clear();
//                Predicates.await( () -> timeStamps.initWrites.size() == timeStamps.handledRequest.size(), 3, TimeUnit.SECONDS );
//                hej.print();
//                hej.clear();
//                System.out.println( "TX SIZE : " + j * muliplicationFactor + " B" );
//                System.out.println( timeStamps.toString() );
//                timeStamps.reset();
//                total.end();
                time1.end();
                System.out.println( j * muliplicationFactor + "\t" + (System.currentTimeMillis() - start) );
//                hej.print();
            }
//            hej.print();

        }
        finally
        {
            Future<?> future1 = serverExecutor.shutdownGracefully();
            Future<?> future2 = clientExecutor.shutdownGracefully();

            Futures.combine( future1, future2 ).get();
        }
    }

    private DistributedOperation createDistributedOperation( byte[] bytes )
    {
        return new DistributedOperation( new ReplicatedTransaction( bytes ), new GlobalSession( UUID.randomUUID(), new MemberId( UUID.randomUUID() ) ),
                new LocalOperationId( 1, 2 ) );
    }

    private RaftMessages.AppendEntries.Request createAppendRequestEntry( ReplicatedContent... replicatedContents )
    {
        RaftLogEntry[] entries = Arrays.stream( replicatedContents ).map( r -> new RaftLogEntry( 1, r ) ).toArray( RaftLogEntry[]::new );
        return new RaftMessages.AppendEntries.Request( new MemberId( UUID.randomUUID() ), 1L, 2L, 3L, entries, 0L );
    }

    private RaftMessages.ClusterIdAwareMessage<RaftMessages.RaftMessage> composeClusterAware( RaftMessages.RaftMessage newEntryRequest )
    {
        return RaftMessages.ClusterIdAwareMessage.of( new ClusterId( UUID.randomUUID() ), newEntryRequest );
    }

    private RaftMessages.NewEntry.Request createNewEntryRequest( ReplicatedContent content )
    {
        return new RaftMessages.NewEntry.Request( new MemberId( UUID.randomUUID() ), content );
    }

    private Bootstrap createClient( NioEventLoopGroup clientExecutor )
    {
        return new Bootstrap().group( clientExecutor ).channel( NioSocketChannel.class )
//                .option( ChannelOption.ALLOCATOR, allocator() )
                .handler( new ChannelInitializer<SocketChannel>()
                {
                    @Override
                    protected void initChannel( SocketChannel ch ) throws Exception
                    {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast( new LengthFieldBasedFrameDecoder( Integer.MAX_VALUE, 0, 4, 0, 4 ) );
                        pipeline.addLast( new LengthFieldPrepender( 4 ) );
                        new RaftMessageCompleteEncoder( new CoreReplicatedContentMarshal() ).addTo( pipeline );
                    }
                } );
    }

    private PooledByteBufAllocator allocator()
    {
        return new PooledByteBufAllocator( true, 0, 8, 8192, 14, PooledByteBufAllocator.defaultTinyCacheSize(), PooledByteBufAllocator.defaultSmallCacheSize(),
                PooledByteBufAllocator.defaultNormalCacheSize(), PooledByteBufAllocator.defaultUseCacheForAllThreads(), 0 );
    }

    private ServerBootstrap createServer( RaftMessageSimpleChannelInboundHandler raftMessageHandler, RaftMessageComposingDecoder raftMessageComposingDecoder,
            NioEventLoopGroup serverExecutor )
    {
        return new ServerBootstrap().group( serverExecutor ).channel( NioServerSocketChannel.class ).childHandler( new ChannelInitializer<SocketChannel>()
        {
            @Override
            protected void initChannel( SocketChannel ch ) throws Exception
            {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast( new LengthFieldBasedFrameDecoder( Integer.MAX_VALUE, 0, 4, 0, 4 ) );
                pipeline.addLast( new LengthFieldPrepender( 4 ) );
                new RaftMessageCompleteDecoder( raftMessageComposingDecoder, Clock.systemUTC(), new CoreReplicatedContentMarshal() ).addTo( pipeline );
                pipeline.addLast( raftMessageHandler );
            }
        } );
    }

    private static class RaftMessageSimpleChannelInboundHandler extends SimpleChannelInboundHandler<RaftMessages.RaftMessage>
    {
        private int count;
        private final Queue<RaftMessages.RaftMessage> received = new LinkedBlockingQueue<>();

        @Override
        protected void channelRead0( ChannelHandlerContext ctx, RaftMessages.RaftMessage msg ) throws Exception
        {
            System.out.println( "msg = " + msg );
            checkResult( msg );
            count++;
        }

        public void reset()
        {
            count = 0;
        }

        public int getCount()
        {
            return count;
        }

        private void checkResult( RaftMessages.RaftMessage given )
        {
            RaftMessages.RaftMessage expected = received.poll();
            if ( expected != null )
            {
                assert given instanceof RaftMessages.ClusterIdAwareMessage;
                assert expected instanceof RaftMessages.ClusterIdAwareMessage;
                assertEquals( ((RaftMessages.ClusterIdAwareMessage) expected).clusterId(), ((RaftMessages.ClusterIdAwareMessage) given).clusterId() );
                assertEquals( ((RaftMessages.ClusterIdAwareMessage) expected).message(), ((RaftMessages.ClusterIdAwareMessage) given).message() );
            }
        }

        public void setExpected( RaftMessages.RaftMessage message )
        {
            received.add( message );
        }
    }

    class ReTransactionChunkHandler extends SimpleChannelInboundHandler<ReplicatedTransactionChunk>
    {
        long size;
        long responses;
        long startTime;

        @Override
        protected void channelRead0( ChannelHandlerContext ctx, ReplicatedTransactionChunk msg ) throws Exception
        {
//            if ( startTime == 0 )
//            {
//                startTime = System.currentTimeMillis();
//            }
//            size += msg.content().readableBytes();
            if ( msg.isLast() )
            {
//                System.out.println( "All bytes after: " + (System.currentTimeMillis() - startTime) );
//                startTime;
                responses++;
//                System.out.println( "size = " + size );
//                size;
            }
//            else
//            {
//                System.out.println( "notlast");
//            }
        }

        long getResponses()
        {
            return responses;
        }

        void reset()
        {
            responses = 0;
        }
    }

    class ReTransactionChunkHandlerBB extends SimpleChannelInboundHandler<ByteBuf>
    {
        long size;
        long responses;
        long startTime;

        @Override
        protected void channelRead0( ChannelHandlerContext ctx, ByteBuf msg ) throws Exception
        {
            if ( startTime == 0 )
            {
                startTime = System.currentTimeMillis();
            }
//            size += msg.content().readableBytes();
            responses += msg.readableBytes() - 1;
//            if ( msg.readBoolean() )
//            {
//                System.out.println( "All bytes after: " + (System.currentTimeMillis() - startTime) );
//                startTime;
//                responses++;
////                System.out.println( "size = " + size );
////                size;
//            }
//            else
//            {
//                System.out.println( "notlast");
//            }
        }

        long getResponses()
        {
            return responses;
        }

        void reset()
        {
            responses = 0;
        }
    }

    class DummyTransaction extends DefaultByteBufHolder
    {
        DummyTransaction( byte[] data )
        {
            super( Unpooled.wrappedBuffer( data ) );
        }

        DummyTransaction( ByteBuf in )
        {
            super( in );
        }
    }

    class DummyTransactionEncoder extends MessageToByteEncoder<DummyTransaction>
    {

        @Override
        protected void encode( ChannelHandlerContext ctx, DummyTransaction msg, ByteBuf out ) throws Exception
        {
            out.writeInt( msg.content().readableBytes() );
            out.writeBytes( msg.content() );
        }
    }

    class DummyTransactionDecoder2 extends MessageToMessageDecoder<DummyTransaction>
    {

        @Override
        protected void decode( ChannelHandlerContext ctx, DummyTransaction msg, List<Object> out ) throws Exception
        {
            System.out.println( "decode" );
        }
    }

    class DummyTransactionDecoder extends ByteToMessageDecoder
    {
        @Override
        protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out ) throws Exception
        {
            if ( in.readableBytes() < 4 )
            {
                return;
            }
            int readerIndex = in.readerIndex();
            int length = in.readInt();
            if ( in.readableBytes() < length )
            {
                in.readerIndex( readerIndex );
                return;
            }
//            System.out.println(length);
            ByteBuf byteBuf = ByteBufUtil.readBytes( ctx.alloc(), in, length );
            out.add( new DummyTransaction( byteBuf ) );
//            int readerIndex = in.readerIndex();
//            if ( readerIndex + 4 > in.writerIndex() )
//            {
//                return;
//            }
//            int length = in.readInt();
//            if ( in.readableBytes() < length )
//            {
////                System.out.println( "INFO" );
////                System.out.println( length );
////                System.out.println( in.readableBytes() );
////                System.out.println( readerIndex );
//                in.readerIndex( readerIndex );
//                return;
//            }
//            byte[] bytes = new byte[length];
//            in.readBytes( bytes, 0, length );
//            out.add( new DummyTransaction( bytes ) );
        }
    }

    class DummyTransactionHandler extends SimpleChannelInboundHandler<DummyTransaction>
    {
        private final TimeStamps timeStamps;
        private int count;

        DummyTransactionHandler( TimeStamps timeStamps )
        {
            this.timeStamps = timeStamps;
        }

        @Override
        protected void channelRead0( ChannelHandlerContext ctx, DummyTransaction msg ) throws Exception
        {
//            System.out.println( count++ );
            timeStamps.requestHandled();
//            msg.content().release();
        }
    }

    class ChunkedInputHanlder extends SimpleChannelInboundHandler<ByteBuf>
    {
        private final TimeStamps timeStamps;
        long counter;

        ChunkedInputHanlder( TimeStamps timeStamps )
        {
            this.timeStamps = timeStamps;
        }

        void clear()
        {
            counter = 0;
        }

        @Override
        protected void channelRead0( ChannelHandlerContext ctx, ByteBuf msg ) throws Exception
        {
//            byte[] bytes = new byte[msg.readableBytes()];
//            msg.readBytes( bytes );
            counter += msg.readableBytes();
//            System.out.println( readable );
//            System.out.println( "Byte buf!" );
        }
    }

    class TimeStamps
    {
        private final List<Long> initWrites = new ArrayList<>();
        private final List<Long> writeSent = new ArrayList<>();
        private final List<Long> handledRequest = new ArrayList<>();

        void initWrite()
        {
            initWrites.add( System.nanoTime() );
        }

        void writeSent()
        {
            writeSent.add( System.nanoTime() );
        }

        void requestHandled()
        {
            handledRequest.add( System.nanoTime() );
        }

        void reset()
        {
            initWrites.clear();
            writeSent.clear();
            handledRequest.clear();
        }

        @Override
        public String toString()
        {
            assert initWrites.size() == writeSent.size();
            assert initWrites.size() == handledRequest.size();
            StringBuilder stringBuilder = new StringBuilder();
            ArrayList<Long> totalSendTime = new ArrayList<>();
            ArrayList<Long> totalHandleTime = new ArrayList<>();
            ArrayList<Long> totalTotalTime = new ArrayList<>();
            for ( int i = 0; i < initWrites.size(); i++ )
            {
                long handleTime = handledRequest.get( i ) - writeSent.get( i );
                long totalTime = handledRequest.get( i ) - initWrites.get( i );
                long sendTime = writeSent.get( i ) - initWrites.get( i );
//                stringBuilder.append( format( "Init write to sent: %d, Sent to handled: %d, Total: %d", sendTime,
//                        handleTime, totalTime ) );
//                stringBuilder.append( "\n" );
                totalHandleTime.add( handleTime );
                totalSendTime.add( sendTime );
                totalTotalTime.add( totalTime );
            }
            stringBuilder.append( "MEANS" );
            stringBuilder.append( "\n" );
            return stringBuilder.toString();
        }
    }
}
