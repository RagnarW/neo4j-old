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
package org.neo4j.causalclustering.core.state.machines.tx;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.EmptyByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;
import io.netty.handler.stream.ChunkedStream;

import java.io.ByteArrayInputStream;

public class ReplicatedTransactionReader implements ChunkedInput<ReplicatedTransactionChunk>
{
    private final ChunkedStream stream;
    private final int length;

    public ReplicatedTransactionReader( ByteArrayInputStream byteArrayInputStream, int length )
    {
        this.stream = new ChunkedStream( byteArrayInputStream );
        this.length = length;
    }

    @Override
    public boolean isEndOfInput() throws Exception
    {
        return stream.isEndOfInput();
    }

    @Override
    public void close() throws Exception
    {
        stream.close();
    }

    @Override
    public ReplicatedTransactionChunk readChunk( ChannelHandlerContext ctx ) throws Exception
    {
        ByteBuf byteBuf = stream.readChunk( ctx );
        return transform( byteBuf );
    }

    @Override
    public ReplicatedTransactionChunk readChunk( ByteBufAllocator allocator ) throws Exception
    {
        ByteBuf byteBuf = stream.readChunk( allocator );
        return transform( byteBuf );
    }

    private ReplicatedTransactionChunk transform( ByteBuf byteBuf ) throws Exception
    {
        boolean lastChunk = isEndOfInput();
        if ( byteBuf == null )
        {
            byteBuf = new EmptyByteBuf( ByteBufAllocator.DEFAULT );
        }
        return new ReplicatedTransactionChunk( byteBuf, lastChunk, length );
    }

    @Override
    public long length()
    {
        return length;
    }

    @Override
    public long progress()
    {
        return stream.progress();
    }
}
