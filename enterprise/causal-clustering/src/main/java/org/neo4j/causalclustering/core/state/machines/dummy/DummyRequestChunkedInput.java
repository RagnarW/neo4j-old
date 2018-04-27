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
package org.neo4j.causalclustering.core.state.machines.dummy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;
import io.netty.handler.stream.ChunkedStream;

import java.io.ByteArrayInputStream;

public class DummyRequestChunkedInput implements ChunkedInput<DummyRequestChunk>
{

    private final ChunkedStream chunkedStream;
    private final int length;

    public DummyRequestChunkedInput( byte[] data )
    {
        length = data.length;
        chunkedStream = new ChunkedStream( new ByteArrayInputStream( data ) );
    }

    @Override
    public boolean isEndOfInput() throws Exception
    {
        return chunkedStream.isEndOfInput();
    }

    @Override
    public void close() throws Exception
    {
        chunkedStream.close();
    }

    @Override
    public DummyRequestChunk readChunk( ChannelHandlerContext ctx ) throws Exception
    {
        ByteBuf byteBuf = chunkedStream.readChunk( ctx );
        return transform( byteBuf );
    }

    @Override
    public DummyRequestChunk readChunk( ByteBufAllocator allocator ) throws Exception
    {
        ByteBuf byteBuf = chunkedStream.readChunk( allocator );
        return transform( byteBuf );
    }

    @Override
    public long length()
    {
        return chunkedStream.length();
    }

    @Override
    public long progress()
    {
        return chunkedStream.progress();
    }

    private DummyRequestChunk transform( ByteBuf byteBuf ) throws Exception
    {
        boolean endOfInput = isEndOfInput();
        return new DummyRequestChunk( byteBuf, endOfInput, length );
    }
}
