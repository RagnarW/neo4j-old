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
package org.neo4j.causalclustering.core.replication;

import java.io.IOException;

import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public interface CompositeReplicatedContent extends Iterable<ReplicatedContent>, ReplicatedContent
{
    CompositeReplicatedContentHeader compositeContentHeader();

    CompositeReplicatedContentHeaderMarshal MARSHAL = new CompositeReplicatedContentHeaderMarshal();

    class CompositeReplicatedContentHeader implements ReplicatedContent
    {
        private final long length;
        private final CompositeReplicatedContentTypes type;

        public CompositeReplicatedContentHeader( long length, CompositeReplicatedContentTypes type )
        {
            this.length = length;
            this.type = type;
        }

        public long length()
        {
            return length;
        }

        public CompositeReplicatedContentTypes type()
        {
            return type;
        }
    }

    class CompositeReplicatedContentHeaderMarshal extends SafeChannelMarshal<CompositeReplicatedContentHeader>
    {

        @Override
        protected CompositeReplicatedContentHeader unmarshal0( ReadableChannel channel ) throws IOException
        {
            CompositeReplicatedContentTypes type = CompositeReplicatedContentTypes.values()[channel.getInt()];
            long length = channel.getLong();
            return new CompositeReplicatedContentHeader( length, type );
        }

        @Override
        public void marshal( CompositeReplicatedContentHeader compositeReplicatedContentHeader, WritableChannel channel ) throws IOException
        {
            channel.putInt( compositeReplicatedContentHeader.type.ordinal() );
            channel.putLong( compositeReplicatedContentHeader.length() );
        }
    }
}
