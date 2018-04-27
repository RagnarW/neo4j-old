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
package org.neo4j.causalclustering.messaging.marshalling.encoding;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;

import org.neo4j.causalclustering.core.consensus.ReplicatedInteger;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntryEntries;
import org.neo4j.causalclustering.core.replication.CompositeReplicatedContent;
import org.neo4j.causalclustering.core.replication.DistributedOperation;
import org.neo4j.causalclustering.core.replication.session.GlobalSession;
import org.neo4j.causalclustering.core.replication.session.LocalOperationId;
import org.neo4j.causalclustering.identity.MemberId;

import static org.junit.Assert.assertEquals;

public class ReplicatedContentDistributorTest
{
    @Test
    public void shouldContainSplitUpContent() throws Exception
    {
        ReplicatedInteger replicatedInteger = ReplicatedInteger.valueOf( 1 );
        MemberId owner = new MemberId( UUID.randomUUID() );
        GlobalSession globalSession = new GlobalSession( UUID.randomUUID(), owner );
        LocalOperationId localOperationId = new LocalOperationId( 1, 2 );
        DistributedOperation distributedOperation = new DistributedOperation( replicatedInteger, globalSession, localOperationId );
        RaftLogEntry entry1 = new RaftLogEntry( 1, distributedOperation );
        RaftLogEntry entry2 = new RaftLogEntry( 1, replicatedInteger );
        RaftLogEntryEntries replicatedContents = new RaftLogEntryEntries( new RaftLogEntry[]{entry1, entry2} );

        ArrayList<Object> objects = new ArrayList<>();
        new ReplicatedContentDistributor().encode( null, replicatedContents, objects );

        Collections.reverse( objects );
        assertEquals( replicatedInteger, objects.remove( 0 ) );
        assertEquals( new RaftLogEntry.Term( 1 ), objects.remove( 0 ) );
        assertEquals( CompositeReplicatedContent.CompositeReplicatedContentHeader.class, objects.remove( 0 ).getClass() );
        assertEquals( DistributedOperation.DistributedContent.class, objects.remove( 0 ).getClass() );
        assertEquals( replicatedInteger, objects.remove( 0 ) );
        assertEquals( CompositeReplicatedContent.CompositeReplicatedContentHeader.class, objects.remove( 0 ).getClass() );
        assertEquals( new RaftLogEntry.Term( 1 ), objects.remove( 0 ) );
        assertEquals( CompositeReplicatedContent.CompositeReplicatedContentHeader.class, objects.remove( 0 ).getClass() );
        assertEquals( CompositeReplicatedContent.CompositeReplicatedContentHeader.class, objects.remove( 0 ).getClass() );
        assertEquals( 0, objects.size() );
    }
}
