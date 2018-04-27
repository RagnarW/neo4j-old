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
package org.neo4j.causalclustering.messaging.marshalling.encoding;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.util.ReferenceCounted;

import java.util.List;

import org.neo4j.causalclustering.core.replication.ChunkedStreamProvider;
import org.neo4j.causalclustering.core.replication.CompositeReplicatedContent;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;

public class ReplicatedContentDistributor extends MessageToMessageEncoder<ReplicatedContent>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, ReplicatedContent msg, List<Object> out )
    {
        if ( msg instanceof ReferenceCounted )
        {
            ((ReferenceCounted) msg).retain();
        }
        else if ( msg instanceof ChunkedStreamProvider )
        {
            out.add( ((ChunkedStreamProvider) msg).getChunkedStream() );
        }
        else if ( msg instanceof CompositeReplicatedContent )
        {
            out.add( ((CompositeReplicatedContent) msg).compositeContentHeader() );
            for ( ReplicatedContent replicatedContent : (Iterable<ReplicatedContent>) msg )
            {
                encode( ctx, replicatedContent, out );
            }
        }
        else
        {
            out.add( msg );
        }
    }
}
