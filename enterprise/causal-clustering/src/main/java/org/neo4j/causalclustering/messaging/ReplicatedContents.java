package org.neo4j.causalclustering.messaging;

import java.io.IOException;
import java.util.Collection;

import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.messaging.marshalling.ReplicatedContentHandler;
import org.neo4j.storageengine.api.WritableChannel;

public interface ReplicatedContents
{
    enum Type
    {
        DISTRIBUTED_OPERATION
    }

    Collection<ReplicatedContent> replicatedContents();

    Type type();

    void encode( WritableChannel channel, ReplicatedContentHandler replicatedContentHandlerr ) throws IOException;
}
