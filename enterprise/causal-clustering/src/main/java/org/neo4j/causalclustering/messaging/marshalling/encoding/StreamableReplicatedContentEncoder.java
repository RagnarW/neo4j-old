package org.neo4j.causalclustering.messaging.marshalling.encoding;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

import org.neo4j.causalclustering.messaging.StreamableReplicatedContent;

public class StreamableReplicatedContentEncoder<T> extends MessageToMessageEncoder<StreamableReplicatedContent<T>>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, StreamableReplicatedContent<T> msg, List<Object> out ) throws Exception
    {
        out.add( (byte) msg.type().ordinal() );
        out.add( msg.chunkedInput() );
    }
}
