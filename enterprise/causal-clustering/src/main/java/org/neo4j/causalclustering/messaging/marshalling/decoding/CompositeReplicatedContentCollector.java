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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;
import java.util.Stack;

import org.neo4j.causalclustering.core.replication.CompositeReplicatedContent;
import org.neo4j.causalclustering.core.replication.CompositeReplicatedContentBuilder;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;

public class CompositeReplicatedContentCollector extends MessageToMessageDecoder<ReplicatedContent>
{
    private Stack<CompositeReplicatedContentBuilder<? extends CompositeReplicatedContent>> builders = new Stack<>();

    @Override
    protected void decode( ChannelHandlerContext ctx, ReplicatedContent msg, List<Object> out )
    {
        if ( msg instanceof CompositeReplicatedContent.CompositeReplicatedContentHeader )
        {
            CompositeReplicatedContent.CompositeReplicatedContentHeader header = (CompositeReplicatedContent.CompositeReplicatedContentHeader) msg;
            CompositeReplicatedContentBuilder<? extends CompositeReplicatedContent> builder = header.type().builder( (int) header.length() );
            if ( builder.isFull() )
            {
                handle( builder.create(), out );
            }
            else
            {
                builders.add( builder );
            }
        }
        else
        {
            handle( msg, out );
        }
    }

    private void handle( ReplicatedContent replicatedContent, List<Object> out )
    {
        ReplicatedContent content = addRecursively( replicatedContent );
        if ( builders.isEmpty() )
        {
            out.add( content );
        }
    }

    private ReplicatedContent addRecursively( ReplicatedContent replicatedContent )
    {
        if ( builders.isEmpty() )
        {
            return replicatedContent;
        }
        CompositeReplicatedContentBuilder<? extends CompositeReplicatedContent> builder = builders.peek();
        builder.add( replicatedContent );
        if ( builder.isFull() )
        {
            CompositeReplicatedContent newReplicatedContent = builders.pop().create();
            return addRecursively( newReplicatedContent );
        }
        else
        {
            return replicatedContent;
        }
    }
}
