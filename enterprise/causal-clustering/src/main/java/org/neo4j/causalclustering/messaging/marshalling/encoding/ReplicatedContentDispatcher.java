package org.neo4j.causalclustering.messaging.marshalling.encoding;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.io.IOException;
import java.util.List;

import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.messaging.ReplicatedContents;
import org.neo4j.causalclustering.messaging.StreamableReplicatedContent;
import org.neo4j.causalclustering.messaging.marshalling.ReplicatedContentType;

public class ReplicatedContentDispatcher extends MessageToMessageEncoder<ReplicatedContent>
{
    @Override
    protected void encode( ChannelHandlerContext channelHandlerContext, ReplicatedContent replicatedContent, List<Object> list ) throws IOException
    {
        if ( replicatedContent instanceof StreamableReplicatedContent )
        {
            list.add( ReplicatedContentType.CHUNKED_CONTENT );
            list.add( replicatedContent );
        }
        if ( replicatedContent instanceof ReplicatedContents )
        {
            list.add( ReplicatedContentType.COMPOSITE_CONTENT );
            list.add( replicatedContent );
            for ( ReplicatedContent content : ((ReplicatedContents) replicatedContent).replicatedContents() )
            {
                encode( channelHandlerContext, content, list );
            }
        }
        else
        {
            list.add( ReplicatedContentType.DIRECT_CONTENT );
            list.add( replicatedContent );
        }
    }
}
