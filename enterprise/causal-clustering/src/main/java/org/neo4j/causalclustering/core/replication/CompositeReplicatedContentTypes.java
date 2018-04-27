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

import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntryEntries;
import org.neo4j.causalclustering.messaging.marshalling.decoding.ReplicateTransactionBuilder;

public enum CompositeReplicatedContentTypes
{
    LOG_ENTRIES
            {
                @Override
                public CompositeReplicatedContentBuilder<RaftLogEntryEntries> builder( int expectedLength )
                {
                    return RaftLogEntryEntries.builder( expectedLength );
                }
            },
    LOG_ENTRY
            {
                @Override
                public CompositeReplicatedContentBuilder<RaftLogEntry> builder( int expectedLength )
                {
                    return RaftLogEntry.builder( expectedLength );
                }
            },
    DISTRIBUTED_OPERATION
            {
                @Override
                public CompositeReplicatedContentBuilder<DistributedOperation> builder( int expectedLength )
                {
                    return DistributedOperation.builder();
                }
            },
    DUMMY_REQUEST
            {
                @Override
                public CompositeReplicatedContentBuilder<? extends CompositeReplicatedContent> builder( int expectedLength )
                {
                    return null;
                }
            };

    public abstract CompositeReplicatedContentBuilder<? extends CompositeReplicatedContent> builder( int expectedLength );
}
