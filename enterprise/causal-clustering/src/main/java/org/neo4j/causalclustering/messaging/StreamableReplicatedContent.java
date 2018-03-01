package org.neo4j.causalclustering.messaging;

import io.netty.handler.stream.ChunkedInput;

public interface StreamableReplicatedContent<CHUNK>
{
    enum Type
    {
        BYTE_BUF_HOLDER;
    }
    ChunkedInput<CHUNK> chunkedInput();

    Type type();
}
