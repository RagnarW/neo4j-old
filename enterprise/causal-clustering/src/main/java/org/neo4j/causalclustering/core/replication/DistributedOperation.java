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
import java.util.Iterator;
import java.util.UUID;

import org.neo4j.causalclustering.core.replication.session.GlobalSession;
import org.neo4j.causalclustering.core.replication.session.LocalOperationId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

/**
 * A uniquely identifiable operation.
 */
public class DistributedOperation implements CompositeReplicatedContent
{
    private final ReplicatedContent content;
    private final DistributedContent distributedContent;

    public DistributedOperation( ReplicatedContent content, GlobalSession globalSession, LocalOperationId operationId )
    {
        this( content, new DistributedContent( globalSession, operationId ) );
    }

    public DistributedOperation( ReplicatedContent replicatedContent, DistributedContent distributedContent )
    {
        this.content = replicatedContent;
        this.distributedContent = distributedContent;
    }

    public GlobalSession globalSession()
    {
        return distributedContent.globalSession;
    }

    public LocalOperationId operationId()
    {
        return distributedContent.operationId;
    }

    public ReplicatedContent content()
    {
        return content;
    }

    @Override
    public boolean hasSize()
    {
        return content.hasSize();
    }

    @Override
    public long size()
    {
        return content.size();
    }

    @Override
    public String toString()
    {
        return "DistributedOperation{" +
               "content=" + content +
               ", globalSession=" + globalSession() +
               ", operationId=" + operationId() +
               '}';
    }

    @Override
    public CompositeReplicatedContentHeader compositeContentHeader()
    {
        return new CompositeReplicatedContentHeader( 1, CompositeReplicatedContentTypes.DISTRIBUTED_OPERATION );
    }

    @Override
    public Iterator<ReplicatedContent> iterator()
    {
        return Iterators.iterator( content, distributedContent );
    }

    public static CompositeReplicatedContentBuilder<DistributedOperation> builder()
    {
        return new Builder();
    }

    public DistributedContent distributedContent()
    {
        return distributedContent;
    }

    public static class DistributedContent implements ReplicatedContent
    {
        private final GlobalSession globalSession;
        private final LocalOperationId operationId;

        DistributedContent( GlobalSession globalSession, LocalOperationId operationId )
        {
            this.globalSession = globalSession;
            this.operationId = operationId;
        }

        public void serialize( WritableChannel channel ) throws IOException
        {
            channel.putLong( globalSession.sessionId().getMostSignificantBits() );
            channel.putLong( globalSession.sessionId().getLeastSignificantBits() );
            new MemberId.Marshal().marshal( globalSession.owner(), channel );

            channel.putLong( operationId.localSessionId() );
            channel.putLong( operationId.sequenceNumber() );
        }

        public static DistributedContent deserialize( ReadableChannel channel ) throws IOException, EndOfStreamException
        {
            long mostSigBits = channel.getLong();
            long leastSigBits = channel.getLong();
            MemberId owner = new MemberId.Marshal().unmarshal( channel );
            GlobalSession globalSession = new GlobalSession( new UUID( mostSigBits, leastSigBits ), owner );

            long localSessionId = channel.getLong();
            long sequenceNumber = channel.getLong();
            LocalOperationId localOperationId = new LocalOperationId( localSessionId, sequenceNumber );

            return new DistributedContent( globalSession, localOperationId );
        }
    }

    private static class Builder implements CompositeReplicatedContentBuilder<DistributedOperation>
    {
        private DistributedContent distributedContent;
        private ReplicatedContent replicatedContent;

        @Override
        public void add( ReplicatedContent replicatedContent )
        {
            if ( replicatedContent instanceof DistributedContent )
            {
                assert distributedContent == null;
                distributedContent = (DistributedContent) replicatedContent;
            }
            else
            {
                assert this.replicatedContent == null;
                this.replicatedContent = replicatedContent;
            }
        }

        @Override
        public boolean isFull()
        {
            return this.replicatedContent != null && this.distributedContent != null;
        }

        @Override
        public DistributedOperation create()
        {
            return new DistributedOperation( replicatedContent, distributedContent.globalSession, distributedContent.operationId );
        }
    }
}
