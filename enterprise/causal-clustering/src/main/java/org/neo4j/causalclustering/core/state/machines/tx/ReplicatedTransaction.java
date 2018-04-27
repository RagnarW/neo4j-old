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

import io.netty.handler.stream.ChunkedInput;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Consumer;

import org.neo4j.causalclustering.core.replication.ChunkedStreamProvider;
import org.neo4j.causalclustering.core.replication.CompositeReplicatedContent;
import org.neo4j.causalclustering.core.replication.CompositeReplicatedContentTypes;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.core.state.CommandDispatcher;
import org.neo4j.causalclustering.core.state.Result;
import org.neo4j.helpers.collection.Iterators;

public class ReplicatedTransaction implements CoreReplicatedContent, ChunkedStreamProvider<ReplicatedTransactionChunk>
{
    private final int length;
    private final byte[] bytes;

    public ReplicatedTransaction( byte[] bytes )
    {
        this.bytes = bytes;
        this.length = bytes.length;
    }

    @Override
    public boolean hasSize()
    {
        return true;
    }

    @Override
    public long size()
    {
        return length;
    }

    @Override
    public void dispatch( CommandDispatcher commandDispatcher, long commandIndex, Consumer<Result> callback )
    {
        commandDispatcher.dispatch( this, commandIndex, callback );
    }

    public ReplicatedTransactionReader getReader()
    {
        return new ReplicatedTransactionReader( new ByteArrayInputStream( bytes ), length );
    }

    public byte[] getTxBytes()
    {
        return bytes;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        ReplicatedTransaction that = (ReplicatedTransaction) o;
        return Arrays.equals( bytes, that.bytes );
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode( bytes );
    }

    @Override
    public ChunkedInput<ReplicatedTransactionChunk> getChunkedStream()
    {
        return getReader();
    }
}
