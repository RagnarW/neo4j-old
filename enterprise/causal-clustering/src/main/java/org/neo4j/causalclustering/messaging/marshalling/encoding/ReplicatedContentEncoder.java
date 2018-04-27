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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.messaging.NetworkFlushableChannelNetty4;
import org.neo4j.causalclustering.messaging.marshalling.RaftMessageComponentState;

/*
    Encodes Replicated Content that do not implement ChunkedInput
 */
public class ReplicatedContentEncoder extends MessageToByteEncoder<ReplicatedContent>
{
    private final SafeChannelMarshal<ReplicatedContent> marshal;

    public ReplicatedContentEncoder( SafeChannelMarshal<ReplicatedContent> marshal )
    {
        this.marshal = marshal;
    }

    @Override
    protected void encode( ChannelHandlerContext ctx, ReplicatedContent msg, ByteBuf out ) throws Exception
    {
        out.writeInt( RaftMessageComponentState.CONTENT.ordinal() );
        marshal.marshal( msg, new NetworkFlushableChannelNetty4( out ) );
    }
}
