package org.neo4j.causalclustering.messaging.marshalling;

import java.io.IOException;

import org.neo4j.causalclustering.core.replication.ReplicatedContent;

@FunctionalInterface
public interface ReplicatedContentHandler
{
    void handle( ReplicatedContent content ) throws IOException;
}
