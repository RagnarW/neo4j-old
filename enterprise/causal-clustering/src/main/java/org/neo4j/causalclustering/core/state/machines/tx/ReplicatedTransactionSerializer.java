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

import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;

import org.neo4j.causalclustering.messaging.marshalling.decoding.ReplicateTransactionBuilder;
import org.neo4j.causalclustering.messaging.marshalling.decoding.ReplicatedTransactionDecoder;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class ReplicatedTransactionSerializer
{
    private ReplicatedTransactionSerializer()
    {
    }

    public static void marshal( ReplicatedTransaction transaction, WritableChannel channel ) throws IOException
    {
        ReplicatedTransactionChunk replicatedTransactionChunk;
        ReplicatedTransactionChunk.Marshal encoder = ReplicatedTransactionChunk.encoder();
        ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
        ReplicatedTransactionReader reader = transaction.getReader();
        try
        {
            do
            {
                replicatedTransactionChunk = reader.readChunk( allocator );
                encoder.encode( replicatedTransactionChunk, channel );
            }
            while ( !replicatedTransactionChunk.isLast() );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    public static ReplicatedTransaction unmarshal( ReadableChannel channel ) throws IOException
    {
        ReplicatedTransactionChunk.UnMarshal<ReplicatedTransactionChunk> decoder = ReplicatedTransactionChunk.decoder(ReplicatedTransactionChunk.initiator());

        ReplicateTransactionBuilder transactionBuilder = ReplicatedTransactionDecoder.builder();
        while ( true )
        {
            if ( transactionBuilder.addChunk( decoder.decode( channel ) ) )
            {
                return transactionBuilder.create();
            }
        }
    }

//    public static void marshal( ReplicatedTransaction transaction, ByteBuf buffer )
//    {
//        byte[] txBytes = transaction.getByteLenth();
//        buffer.writeInt( txBytes.length );
//        buffer.writeBytes( txBytes );
//    }
//
//    public static ReplicatedTransaction unmarshal( ByteBuf buffer )
//    {
//        int txBytesLength = buffer.readInt();
//        byte[] txBytes = new  byte[txBytesLength];
//        buffer.readBytes( txBytes );
//
//        return new ReplicatedTransaction( txBytes );
//    }
}
