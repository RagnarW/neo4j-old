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
import io.netty.buffer.DefaultByteBufHolder;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.IOException;
import java.util.List;

import org.neo4j.causalclustering.core.replication.ChunkedReplicatedContent;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public abstract class CompleteAwareByteBufChunk extends DefaultByteBufHolder implements ChunkedReplicatedContent
{
    private final boolean lastChunk;
    public final int totalLength;

    public CompleteAwareByteBufChunk( ByteBuf byteBuf, boolean lastChunk, int totalLength )
    {
        super( byteBuf );
        this.lastChunk = lastChunk;
        this.totalLength = totalLength;
    }

    public static Marshal encoder()
    {
        return new Marshal();
    }

    public static <T extends CompleteAwareByteBufChunk> UnMarshal decoder( Initiator<T> initiator )
    {
        return new UnMarshal<>( initiator );
    }

    public boolean isLast()
    {
        return lastChunk;
    }

    public static class Marshal extends MessageToByteEncoder<CompleteAwareByteBufChunk>
    {
        @Override
        protected void encode( ChannelHandlerContext ctx, CompleteAwareByteBufChunk msg, ByteBuf out )
        {
            ChunkedReplicatedContent.encodeHeader( msg, out );
            out.writeBoolean( msg.isLast() );
            out.writeInt( msg.totalLength );
            out.writeInt( msg.content().readableBytes() );
            ByteBuf slice = msg.content().slice();
            byte[] bytes = new byte[msg.content().readableBytes()];
            slice.readBytes( bytes );
            out.writeBytes( msg.content() );
        }

        protected void encode( CompleteAwareByteBufChunk msg, WritableChannel writableChannel ) throws IOException
        {
            writableChannel.put( (byte) (msg.isLast() ? 1 : 0) );
            writableChannel.putInt( msg.totalLength );
            int length = msg.content().readableBytes();
            writableChannel.putInt( length );
            byte[] bytes = new byte[length];
            msg.content().readBytes( bytes );
            writableChannel.put( bytes, length );
            msg.release();
        }
    }

    public static class UnMarshal<T extends CompleteAwareByteBufChunk> extends ByteToMessageDecoder
    {
        private Initiator<T> initiator;

        UnMarshal( Initiator<T> initiator )
        {
            this.initiator = initiator;
        }

        @Override
        public void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out )
        {
            boolean isLast = in.readBoolean();
            int totalLength = in.readInt();
            int length = in.readInt();
            ByteBuf slice = in.retainedSlice( in.readerIndex(), in.readableBytes() );
            in.readerIndex( in.readerIndex() + in.readableBytes() );
            byte[] bytes = new byte[length];
            slice.readBytes( bytes );
            slice.readerIndex( 0 );
            out.add( initiator.init( slice, isLast, totalLength ) );
        }

        public T decode( ReadableChannel channel ) throws IOException
        {
            boolean isLast = channel.get() == 1;
            int totalLength = channel.getInt();
            int length = channel.getInt();
            byte[] bytes = new byte[length];
            channel.get( bytes, length );
            ByteBuf byteBuf = Unpooled.wrappedBuffer( bytes );
            return initiator.init( byteBuf, isLast, totalLength );
        }
    }

    public interface Initiator<T extends CompleteAwareByteBufChunk>
    {
        T init( ByteBuf byteBuf, boolean isLast, int totalLength );
    }
}
