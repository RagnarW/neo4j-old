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
package org.neo4j.causalclustering.core.consensus.log;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import org.neo4j.causalclustering.core.replication.CompositeReplicatedContentTypes;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.core.replication.CompositeReplicatedContent;
import org.neo4j.causalclustering.core.replication.CompositeReplicatedContentBuilder;
import org.neo4j.helpers.collection.Iterators;

public class RaftLogEntryEntries implements CompositeReplicatedContent
{
    public static final RaftLogEntryEntries empty = new RaftLogEntryEntries( RaftLogEntry.empty );
    private final RaftLogEntry[] raftLogEntries;

    public RaftLogEntryEntries( RaftLogEntry[] raftLogEntries )
    {
        this.raftLogEntries = raftLogEntries;
    }

    @Override
    public boolean hasSize()
    {
        return true;
    }

    @Override
    public String toString()
    {
        return "RaftLogEntryEntries{" + "raftLogEntries=" + Arrays.toString( raftLogEntries ) + '}';
    }

    @Override
    public Iterator<ReplicatedContent> iterator()
    {
        return Iterators.iterator( raftLogEntries );
    }

    @Override
    public CompositeReplicatedContentHeader compositeContentHeader()
    {
        return new CompositeReplicatedContentHeader( raftLogEntries.length, CompositeReplicatedContentTypes.LOG_ENTRIES );
    }

    public static CompositeReplicatedContentBuilder<RaftLogEntryEntries> builder( int expectedSize )
    {
        return new Builder( expectedSize );
    }

    public RaftLogEntry[] entries()
    {
        return raftLogEntries;
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
        RaftLogEntryEntries that = (RaftLogEntryEntries) o;
        return Arrays.equals( raftLogEntries, that.raftLogEntries );
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode( raftLogEntries );
    }

    public static class Builder implements CompositeReplicatedContentBuilder<RaftLogEntryEntries>
    {
        private final int expectedSize;
        List<RaftLogEntry> raftLogEntries = new LinkedList<>();

        private Builder( int expectedSize )
        {
            this.expectedSize = expectedSize;
        }

        @Override
        public void add( ReplicatedContent replicatedContent )
        {
            assert replicatedContent instanceof RaftLogEntry;
            assert raftLogEntries.size() < expectedSize;
            raftLogEntries.add( (RaftLogEntry) replicatedContent );
        }

        @Override
        public boolean isFull()
        {
            return expectedSize == raftLogEntries.size();
        }

        @Override
        public RaftLogEntryEntries create()
        {
            return new RaftLogEntryEntries( raftLogEntries.toArray( new RaftLogEntry[raftLogEntries.size()] ) );
        }
    }
}
