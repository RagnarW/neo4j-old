package org.neo4j.causalclustering.messaging.marshalling;

public class ReplicatedContentType
{
    public static final byte CHUNKED_CONTENT = (byte) 0;
    public static final byte COMPOSITE_CONTENT = (byte) 1;
    public static final byte DIRECT_CONTENT = (byte) 2;
}
