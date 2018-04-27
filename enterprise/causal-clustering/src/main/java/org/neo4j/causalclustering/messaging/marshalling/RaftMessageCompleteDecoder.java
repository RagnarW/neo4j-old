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

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.messaging.ChannelHandlerAppender;
import org.neo4j.causalclustering.messaging.marshalling.decoding.CompositeReplicatedContentCollector;
import org.neo4j.causalclustering.messaging.marshalling.decoding.DummyRequestDecoder;
import org.neo4j.causalclustering.messaging.marshalling.decoding.RaftMessageComponentDispatcher;
import org.neo4j.causalclustering.messaging.marshalling.decoding.RaftMessageComposingDecoder;
import org.neo4j.causalclustering.messaging.marshalling.decoding.ReplicatedTransactionDecoder;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

public class RaftMessageCompleteDecoder implements ChannelHandlerAppender
{
    private final RaftMessageComposingDecoder composingDecoder;
    private final Clock clock;
    private final ChannelMarshal<ReplicatedContent> replicatedContentChannelMarshal;
    private final Log log;

    public RaftMessageCompleteDecoder( RaftMessageComposingDecoder composingDecoder, Clock clock,
            ChannelMarshal<ReplicatedContent> replicatedContentChannelMarshal )
    {
        this( composingDecoder, clock, replicatedContentChannelMarshal, NullLog.getInstance() );
    }

    public RaftMessageCompleteDecoder( RaftMessageComposingDecoder composingDecoder, Clock clock,
            ChannelMarshal<ReplicatedContent> replicatedContentChannelMarshal, Log log )
    {
        this.composingDecoder = composingDecoder;
        this.clock = clock;
        this.replicatedContentChannelMarshal = replicatedContentChannelMarshal;
        this.log = log;
    }

    public void addTo( ChannelPipeline pipeline )
    {
//        pipeline.addLast( new RaftMessageComponentDispatcher( clock, replicatedContentChannelMarshal ) );
//        pipeline.addLast( new ReplicatedTransactionDecoder() );
//        pipeline.addLast( new CompositeReplicatedContentCollector() );
//        composingDecoder.addTo( pipeline );
        pipeline.addLast( handlers() );
    }

    @Override
    public ChannelHandler[] handlers()
    {
        List<ChannelHandler> channelHandlers =
                Arrays.asList( new RaftMessageComponentDispatcher( clock, replicatedContentChannelMarshal, log ), new ReplicatedTransactionDecoder(),
                        new DummyRequestDecoder(),
                        new CompositeReplicatedContentCollector() );

        ArrayList<ChannelHandler> all = new ArrayList<>();
        all.addAll( channelHandlers );
        all.addAll( Arrays.asList( composingDecoder.handlers() ) );

        return all.toArray( new ChannelHandler[]{} );
    }
}
