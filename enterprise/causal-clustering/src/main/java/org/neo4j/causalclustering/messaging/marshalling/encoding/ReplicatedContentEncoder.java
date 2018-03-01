package org.neo4j.causalclustering.messaging.marshalling.encoding;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.IOException;

import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.messaging.NetworkFlushableChannelNetty4;
import org.neo4j.causalclustering.messaging.ReplicatedContents;
import org.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;

public class ReplicatedContentEncoder extends MessageToByteEncoder<ReplicatedContent>
{
    private final ChannelMarshal<ReplicatedContent> marshal;

    public ReplicatedContentEncoder( ChannelMarshal<ReplicatedContent> marshal )
    {
        this.marshal = marshal;
    }

    @Override
    protected void encode( ChannelHandlerContext channelHandlerContext, ReplicatedContent replicatedContent, ByteBuf byteBuf ) throws IOException
    {
        NetworkFlushableChannelNetty4 writableChannel = new NetworkFlushableChannelNetty4( byteBuf );
        if ( (replicatedContent instanceof ReplicatedContents) )
        {
            writableChannel.put( (byte) ((ReplicatedContents) replicatedContent).type().ordinal() );
            // We do not handle the content owned by the this replicated contents. It is instead handled in the recursion in {@link ReplicatedContentDispatcher}
            ((ReplicatedContents) replicatedContent).encode( writableChannel, content ->
            {
            } );
        }
        else
        {
            marshal.marshal( replicatedContent, writableChannel );
        }
    }
}
