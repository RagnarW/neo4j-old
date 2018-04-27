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

import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

import org.neo4j.causalclustering.core.replication.DistributedOperation;
import org.neo4j.causalclustering.core.replication.session.GlobalSession;
import org.neo4j.causalclustering.core.replication.session.LocalOperationId;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.CoreReplicatedContentMarshal;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;

public class EntryRecordTest
{
    @Test
    public void name() throws IOException, EndOfStreamException
    {
        InMemoryClosableChannel inMemoryClosableChannel = new InMemoryClosableChannel();
        CoreReplicatedContentMarshal coreReplicatedContentMarshal = new CoreReplicatedContentMarshal();
        ReplicatedTransaction tx = new ReplicatedTransaction( new byte[]{1, 2, 3, 4} );
        DistributedOperation replicatedContents =
                new DistributedOperation( tx, new GlobalSession( UUID.randomUUID(), new MemberId( UUID.randomUUID() ) ), new LocalOperationId( 1, 2 ) );
        EntryRecord.write( inMemoryClosableChannel, coreReplicatedContentMarshal, 5, 2, replicatedContents );
        EntryRecord read = EntryRecord.read( inMemoryClosableChannel, coreReplicatedContentMarshal );
        System.out.println( read );
    }
}
