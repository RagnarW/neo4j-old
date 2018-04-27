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
package org.neo4j.causalclustering.messaging.marshalling.decoding;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.messaging.ChannelHandlerAppender;
import org.neo4j.causalclustering.messaging.marshalling.RaftMessageHeader;

public class RaftMessageComposingDecoder implements ChannelHandlerAppender
{

    private final RaftMessageHeaderHandler raftMessageHeaderHandler;
    private final ReplicatedContentHandler replicatedContentHandler;
    private RaftMessages.ClusterIdAwareMessage awaitingContent;

    public RaftMessageComposingDecoder()
    {
        raftMessageHeaderHandler = new RaftMessageHeaderHandler( this );
        replicatedContentHandler = new ReplicatedContentHandler( this );
        awaitingContent = null;
    }

    private void onHeader( RaftMessageHeader raftMessageHeader, List<Object> out )
    {
        check();
        RaftMessages.ClusterIdAwareMessage message = raftMessageHeader.message();
        if ( message.hasContent() )
        {
            awaitingContent = message;
        }
        else
        {
            out.add( message );
        }
    }

    private void check()
    {
        if ( awaitingContent != null )
        {
            System.out.println("NAH");
        }
        assert awaitingContent == null;
    }

    private void onContent( ReplicatedContent replicatedContent, List<Object> out )
    {
        assert awaitingContent != null;
        ((RaftMessages.ContentIncludedRaftMessage) awaitingContent.message()).replaceContent( replicatedContent );
        out.add( awaitingContent );
        awaitingContent = null;
    }

    @Override
    public void addTo( ChannelPipeline pipeline )
    {
        pipeline.addLast( handlers() );
    }

    @Override
    public ChannelHandler[] handlers()
    {
        return new ChannelHandler[]{raftMessageHeaderHandler, replicatedContentHandler};
    }

    private class RaftMessageHeaderHandler extends MessageToMessageDecoder<RaftMessageHeader>
    {
        private final RaftMessageComposingDecoder raftMessageComposingDecoder;

        RaftMessageHeaderHandler( RaftMessageComposingDecoder raftMessageComposingDecoder )
        {
            this.raftMessageComposingDecoder = raftMessageComposingDecoder;
        }

        @Override
        protected void decode( ChannelHandlerContext ctx, RaftMessageHeader msg, List<Object> out ) throws Exception
        {
            raftMessageComposingDecoder.onHeader( msg, out );
        }
    }

    private class ReplicatedContentHandler extends MessageToMessageDecoder<ReplicatedContent>
    {

        private final RaftMessageComposingDecoder raftMessageComposingDecoder;

        ReplicatedContentHandler( RaftMessageComposingDecoder raftMessageComposingDecoder )
        {
            this.raftMessageComposingDecoder = raftMessageComposingDecoder;
        }

        @Override
        protected void decode( ChannelHandlerContext ctx, ReplicatedContent msg, List<Object> out ) throws Exception
        {
            raftMessageComposingDecoder.onContent( msg, out );
        }
    }
}
