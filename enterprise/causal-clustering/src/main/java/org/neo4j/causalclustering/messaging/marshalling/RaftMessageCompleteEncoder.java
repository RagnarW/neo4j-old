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
package org.neo4j.causalclustering.messaging.marshalling;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.stream.ChunkedWriteHandler;

import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionChunk;
import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.messaging.ChannelHandlerAppender;
import org.neo4j.causalclustering.messaging.marshalling.encoding.CompositeReplicatedContentEncoder;
import org.neo4j.causalclustering.messaging.marshalling.encoding.RaftMessageDistributor;
import org.neo4j.causalclustering.messaging.marshalling.encoding.RaftMessageHeaderEncoder;
import org.neo4j.causalclustering.messaging.marshalling.encoding.ReplicatedContentDistributor;
import org.neo4j.causalclustering.messaging.marshalling.encoding.ReplicatedContentEncoder;

public class RaftMessageCompleteEncoder implements ChannelHandlerAppender
{
    private final SafeChannelMarshal<ReplicatedContent> replicatedContentChannelMarshal;

    public RaftMessageCompleteEncoder( SafeChannelMarshal<ReplicatedContent> replicatedContentChannelMarshal )
    {
        this.replicatedContentChannelMarshal = replicatedContentChannelMarshal;
    }

    @Override
    public void addTo( ChannelPipeline pipeline )
    {
//        pipeline.addLast( new RaftMessageHeaderEncoder() );
//        pipeline.addLast( ReplicatedTransactionChunk.encoder() );
////        pipeline.addLast( new ChunkedReplicatedContentEncoder() );
//        pipeline.addLast( new ReplicatedContentEncoder( replicatedContentChannelMarshal ) );
//        pipeline.addLast( new CompositeReplicatedContentEncoder() );
//        pipeline.addLast( new ChunkedWriteHandler() );
//        pipeline.addLast( new ReplicatedContentDistributor() );
//        pipeline.addLast( new RaftMessageDistributor() );
        pipeline.addLast( handlers() );
    }

    @Override
    public ChannelHandler[] handlers()
    {
        return new ChannelHandler[]{
                new RaftMessageHeaderEncoder(),
                ReplicatedTransactionChunk.encoder(),
                new ReplicatedContentEncoder( replicatedContentChannelMarshal ),
                new CompositeReplicatedContentEncoder(),
                new ChunkedWriteHandler(),
                new ReplicatedContentDistributor(),
                new RaftMessageDistributor() };
    }
}
