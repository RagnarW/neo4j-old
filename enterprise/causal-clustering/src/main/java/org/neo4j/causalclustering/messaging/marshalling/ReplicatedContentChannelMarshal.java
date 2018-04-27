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
package org.neo4j.causalclustering.messaging.marshalling;

import java.io.IOException;

import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.storageengine.api.WritableChannel;

public abstract class ReplicatedContentChannelMarshal extends SafeChannelMarshal<ReplicatedContent>
{
    @Override
    public final void marshal( ReplicatedContent replicatedContent, WritableChannel channel ) throws IOException
    {
        channel.putInt( RaftMessageComponentState.CONTENT.ordinal() );
        marshal0( replicatedContent, channel );
    }

    protected abstract void marshal0( ReplicatedContent replicatedContent, WritableChannel channel ) throws IOException;
}
