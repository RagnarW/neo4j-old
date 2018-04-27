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

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import java.time.Clock;
import java.util.Arrays;
import java.util.UUID;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.CoreReplicatedContentMarshal;
import org.neo4j.causalclustering.messaging.marshalling.RaftMessageCompleteDecoder;
import org.neo4j.causalclustering.messaging.marshalling.RaftMessageCompleteEncoder;
import org.neo4j.causalclustering.messaging.marshalling.decoding.RaftMessageComposingDecoder;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;

import static org.junit.Assert.assertEquals;

public class ReplicatedTransactionFactoryTest
{
    @Test
    public void shouldCreateAndExtract()
    {
        LogEntryCommand logEntryCommand1 = new LogEntryCommand( new Command.NodeCommand( new NodeRecord( 42 ), new NodeRecord( 43 ) ) );
        LogEntryCommand logEntryCommand2 = new LogEntryCommand( new Command.LabelTokenCommand( new LabelTokenRecord( -1 ), new LabelTokenRecord( 43 ) ) );
        PhysicalTransactionRepresentation txRepresentation =
                new PhysicalTransactionRepresentation( Arrays.asList( logEntryCommand1.getCommand(), logEntryCommand2.getCommand() ) );
        txRepresentation.setHeader( new byte[]{0, 0, 0, 10}, 1, 2, 3, 4, 5, 6 );

        ReplicatedTransaction immutableReplicatedTransaction = ReplicatedTransactionFactory.createImmutableReplicatedTransaction( txRepresentation );

        EmbeddedChannel encoderChannel = new EmbeddedChannel();
        EmbeddedChannel decoderChannel = new EmbeddedChannel();
        new RaftMessageCompleteEncoder( new CoreReplicatedContentMarshal() ).addTo( encoderChannel.pipeline() );
        new RaftMessageCompleteDecoder( new RaftMessageComposingDecoder(), Clock.systemUTC(), new CoreReplicatedContentMarshal() ).addTo(
                decoderChannel.pipeline() );

        RaftMessages.ClusterIdAwareMessage<RaftMessages.NewEntry.Request> of = RaftMessages.ClusterIdAwareMessage.of( new ClusterId( UUID.randomUUID() ),
                new RaftMessages.NewEntry.Request( new MemberId( UUID.randomUUID() ), immutableReplicatedTransaction ) );
        encoderChannel.writeOutbound( of );

        Object inbound;
        while ( (inbound = encoderChannel.readOutbound()) != null )
        {
            decoderChannel.writeInbound( inbound );
        }
        Object o = decoderChannel.readInbound();
        ReplicatedContent content = ((RaftMessages.ContentIncludedRaftMessage) ((RaftMessages.ClusterIdAwareMessage) o).message()).content();

        assertEquals( content, immutableReplicatedTransaction );
        TransactionRepresentation transactionRepresentation = ReplicatedTransactionFactory.extractTransactionRepresentation( immutableReplicatedTransaction,
                new byte[(int) immutableReplicatedTransaction.size()] );

        assertEquals( txRepresentation, transactionRepresentation );
    }
}
