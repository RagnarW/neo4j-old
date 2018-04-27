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
package org.neo4j.causalclustering.messaging.marshalling.decoding;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.time.Clock;
import java.util.List;

import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import org.neo4j.causalclustering.messaging.marshalling.RaftMessageComponentState;
import org.neo4j.logging.Log;

public class RaftMessageComponentDispatcher extends ByteToMessageDecoder
{
    private final RaftMessageHeaderDecoder raftMessageHeaderDecoder;
    private final ReplicatedContentDecoder replicatedContentDecoder;
    private Log log;
    private final ChunkedReplicatedContentDispatcher chunkedReplicatedContent;
    private final CompositeReplicatedContentDecoder compositeReplicatedContentDecoder;

    public RaftMessageComponentDispatcher( Clock clock, ChannelMarshal<ReplicatedContent> replicatedContentChannelMarshal, Log log )
    {
        this.raftMessageHeaderDecoder = new RaftMessageHeaderDecoder( clock );
        this.replicatedContentDecoder = new ReplicatedContentDecoder( replicatedContentChannelMarshal );
        this.log = log;
        this.chunkedReplicatedContent = new ChunkedReplicatedContentDispatcher( log );
        this.compositeReplicatedContentDecoder = new CompositeReplicatedContentDecoder();
    }

    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out ) throws Exception
    {
        RaftMessageComponentState state = readState( in );

        switch ( state )
        {
        case HEADER:
            raftMessageHeaderDecoder.decode( ctx, in, out );
            break;
        case CONTENT:
            replicatedContentDecoder.decode( ctx, in, out );
            break;
        case CHUNKED_CONTENT:
            chunkedReplicatedContent.decode( ctx, in, out );
            break;
        case COMPOSITE_HEADER:
            compositeReplicatedContentDecoder.decode( ctx, in, out );
            break;
        default:
            throw new IllegalStateException( "Not a recognisable raft message state " + state );
        }
    }

    private RaftMessageComponentState readState( ByteBuf in )
    {
        return RaftMessageComponentState.values()[in.readInt()];
    }
}
