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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import org.neo4j.causalclustering.core.replication.CompositeReplicatedContentBuilder;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionChunk;

public class ReplicateTransactionBuilder
{
    private ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer();
    private boolean complete;

    public boolean addChunk( ReplicatedTransactionChunk chunk )
    {
        if ( complete )
        {
            throw new IllegalStateException( "Cannot add chunk. Builder is complete!" );
        }
        byteBuf.writeBytes( chunk.content() );
        return complete = chunk.isLast();
    }

    public ReplicatedTransaction create()
    {
        if ( !complete )
        {
            throw new IllegalStateException( "Cannot create. Builder is incomplete" );
        }
        byte[] bytes = new byte[byteBuf.writerIndex()];
        byteBuf.readBytes( bytes );
        byteBuf.release();
        return new ReplicatedTransaction( bytes );
    }
}
