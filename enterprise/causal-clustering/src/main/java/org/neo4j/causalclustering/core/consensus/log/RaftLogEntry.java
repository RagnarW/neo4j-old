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

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;

import org.neo4j.causalclustering.core.replication.CompositeReplicatedContent;
import org.neo4j.causalclustering.core.replication.CompositeReplicatedContentBuilder;
import org.neo4j.causalclustering.core.replication.CompositeReplicatedContentTypes;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

import static java.lang.String.format;

public class RaftLogEntry implements CompositeReplicatedContent
{
    public static final RaftLogEntry[] empty = new RaftLogEntry[0];

    private final Term term;
    private final ReplicatedContent content;

    public RaftLogEntry( long term, ReplicatedContent content )
    {
        Objects.requireNonNull( content );
        this.term = new Term( term );
        this.content = content;
    }

    public long term()
    {
        return this.term.term;
    }

    @Override
    public long size()
    {
        return content.size() + 1;
    }

    @Override
    public boolean hasSize()
    {
        return true;
    }

    public ReplicatedContent content()
    {
        return this.content;
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

        RaftLogEntry that = (RaftLogEntry) o;

        return term.equals( that.term ) && content.equals( that.content );
    }

    @Override
    public int hashCode()
    {
        int result = (int) (term.term ^ (term.term >>> 32));
        result = 31 * result + content.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return format( "{term=%d, content=%s}", term.term, content );
    }

    @Override
    public CompositeReplicatedContentHeader compositeContentHeader()
    {
        return new CompositeReplicatedContentHeader( 1, CompositeReplicatedContentTypes.LOG_ENTRY );
    }

    @Override
    public Iterator<ReplicatedContent> iterator()
    {
        return Iterators.iterator( term, content );
    }

    public static CompositeReplicatedContentBuilder<RaftLogEntry> builder( int expectedLength )
    {
        return new Builder();
    }

    private static class Builder implements CompositeReplicatedContentBuilder<RaftLogEntry>
    {
        private Term term;
        private ReplicatedContent replicatedContent;

        @Override
        public void add( ReplicatedContent replicatedContent )
        {
            if ( replicatedContent instanceof Term )
            {
                assert term == null;
                term = (Term) replicatedContent;
            }
            else
            {
                assert replicatedContent != null;
                this.replicatedContent = replicatedContent;
            }
        }

        @Override
        public boolean isFull()
        {
            return replicatedContent != null && term != null;
        }

        @Override
        public RaftLogEntry create()
        {
            return new RaftLogEntry( term.term, replicatedContent );
        }
    }

    public static class Term implements ReplicatedContent
    {
        private final long term;

        public Term( long term )
        {
            this.term = term;
        }

        public static void marshal( Term content, WritableChannel channel ) throws IOException
        {
            channel.putLong( content.term );
        }

        public static Term unmarshal( ReadableChannel channel ) throws IOException
        {
            return new Term( channel.getLong() );
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
            Term term1 = (Term) o;
            return term == term1.term;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( term );
        }
    }
}
