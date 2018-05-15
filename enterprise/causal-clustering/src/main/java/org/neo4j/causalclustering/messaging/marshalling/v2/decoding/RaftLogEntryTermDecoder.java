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
package org.neo4j.causalclustering.messaging.marshalling.v2.decoding;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import org.neo4j.causalclustering.catchup.Protocol;
import org.neo4j.causalclustering.messaging.marshalling.v2.ContentType;

class RaftLogEntryTermDecoder extends ByteToMessageDecoder
{
    private final Protocol<ContentType> protocol;

    RaftLogEntryTermDecoder( Protocol<ContentType> protocol )
    {
        this.protocol = protocol;
    }

    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out )
    {
        long l = in.readLong();
        out.add( new RaftLogEntryTerm( l ) );
        protocol.expect( ContentType.MessageType );
    }

    class RaftLogEntryTerm
    {
        private final long term;

        RaftLogEntryTerm( long term )
        {
            this.term = term;
        }

        public long term()
        {
            return term;
        }
    }
}
