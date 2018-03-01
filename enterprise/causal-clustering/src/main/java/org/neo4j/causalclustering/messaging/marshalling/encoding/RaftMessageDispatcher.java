package org.neo4j.causalclustering.messaging.marshalling.encoding;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;

public class RaftMessageDispatcher extends MessageToMessageEncoder<RaftMessages.RaftMessage>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, RaftMessages.RaftMessage msg, List<Object> out ) throws Exception
    {
        out.add( msg );
        ReplicatedContent content = msg.content();
        if ( content != null )
        {
            out.add( content );
        }
    }
}
