/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 *   SPDX-License-Identifier: Apache-2.0
 */

package org.apache.jena.grpc;

import java.util.function.Consumer;
import java.util.stream.Stream;

import io.grpc.Channel;
import org.apache.jena.graph.Node;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdflink.RDFConnectionAdapter;
import org.apache.jena.rdfpatch.RDFChanges;
import org.apache.jena.rdfpatch.RDFPatch;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;

/**
 * {@link RDFConnection} adapter backed by an {@link RDFLinkGrpc}, providing full
 * Jena Model/Dataset API compatibility alongside gRPC-native {@link #findStream},
 * {@link #applyPatch}, and {@link #subscribePatches} operations.
 */
public class RDFConnectionGrpc extends RDFConnectionAdapter {

    private static final String ERR_LINK_NULL = "link must not be null";

    private final RDFLinkGrpc linkGrpc;

    /**
     * Create an {@link RDFConnectionGrpc} wrapping the given {@link RDFLinkGrpc}.
     *
     * @param link the underlying {@link RDFLinkGrpc}
     * @throws IllegalArgumentException Thrown if {@code link} is {@code null}
     */
    public RDFConnectionGrpc(RDFLinkGrpc link) {
        super(requireLink(link));
        this.linkGrpc = link;
    }

    private static RDFLinkGrpc requireLink(RDFLinkGrpc link) {
        if ( link == null )
            throw new IllegalArgumentException(ERR_LINK_NULL);
        return link;
    }

    /**
     * Connect to a remote {@link RDFGrpcServer} over Netty TCP transport.
     *
     * @param host the server hostname or IP address
     * @param port the server TCP port
     * @return the connected {@link RDFConnectionGrpc}
     */
    public static RDFConnectionGrpc connect(String host, int port) {
        return new RDFConnectionGrpc(RDFLinkGrpc.connect(host, port));
    }

    /**
     * Connect to an in-process {@link RDFGrpcServer} by its server name.
     *
     * @param serverName the in-process server name
     * @return the connected {@link RDFConnectionGrpc}
     */
    public static RDFConnectionGrpc connectInProcess(String serverName) {
        return new RDFConnectionGrpc(RDFLinkGrpc.connectInProcess(serverName));
    }

    /**
     * Connect to an in-process {@link RDFGrpcServer} by its server name with an explicit
     * SPARQL parse-check setting.
     *
     * @param serverName the in-process server name
     * @param parseCheckSPARQL {@code true} to syntax-check SPARQL strings locally
     * @return the connected {@link RDFConnectionGrpc}
     */
    public static RDFConnectionGrpc connectInProcess(String serverName, boolean parseCheckSPARQL) {
        return new RDFConnectionGrpc(
            RDFLinkGrpc.newBuilder()
                .inProcessName(serverName)
                .parseCheckSPARQL(parseCheckSPARQL)
                .build());
    }

    /**
     * Wrap an existing gRPC {@link Channel} in an {@link RDFConnectionGrpc}.
     *
     * @param channel the gRPC channel
     * @return the {@link RDFConnectionGrpc} wrapping {@code channel}
     */
    public static RDFConnectionGrpc connect(Channel channel) {
        return new RDFConnectionGrpc(RDFLinkGrpc.connect(channel));
    }

    /**
     * Return the underlying {@link RDFLinkGrpc}.
     *
     * @return the underlying {@link RDFLinkGrpc}
     */
    public RDFLinkGrpc getLinkGrpc() {
        return linkGrpc;
    }

    /**
     * Stream quads matching {@code (g, s, p, o)} from the remote dataset using {@code RDF_ANY}
     * wildcards for {@code null} or {@link Node#ANY} components.
     *
     * @param g graph node pattern, or {@code null} / {@link Node#ANY} for any graph
     * @param s subject node pattern, or {@code null} / {@link Node#ANY} for any subject
     * @param p predicate node pattern, or {@code null} / {@link Node#ANY} for any predicate
     * @param o object node pattern, or {@code null} / {@link Node#ANY} for any object
     * @return a {@link Stream} of matching {@link Quad} instances
     */
    public Stream<Quad> findStream(Node g, Node s, Node p, Node o) {
        return linkGrpc.findStream(g, s, p, o);
    }

    /**
     * Stream quads matching {@code (g, s, p, o)} from the remote dataset into {@code dest}.
     *
     * @param g graph node pattern, or {@code null} / {@link Node#ANY} for any graph
     * @param s subject node pattern, or {@code null} / {@link Node#ANY} for any subject
     * @param p predicate node pattern, or {@code null} / {@link Node#ANY} for any predicate
     * @param o object node pattern, or {@code null} / {@link Node#ANY} for any object
     * @param dest the destination {@link StreamRDF} sink
     */
    public void findStream(Node g, Node s, Node p, Node o, StreamRDF dest) {
        linkGrpc.findStream(g, s, p, o, dest);
    }

    /**
     * Apply an {@link RDFPatch} to the remote dataset by streaming {@code RDF_Patch_Row} messages.
     *
     * @param patch the patch to apply
     */
    public void applyPatch(RDFPatch patch) {
        linkGrpc.applyPatch(patch);
    }

    /**
     * Apply changes emitted to an {@link RDFChanges} sink to the remote dataset.
     *
     * @param producer callback that emits patch changes
     */
    public void applyPatch(Consumer<RDFChanges> producer) {
        linkGrpc.applyPatch(producer);
    }

    /**
     * Subscribe to live {@code RDF_Patch_Row} updates published by the remote server.
     *
     * @param target the destination {@link RDFChanges} sink
     * @return a {@link RDFLinkGrpc.PatchSubscription} handle that cancels the subscription when closed
     */
    public RDFLinkGrpc.PatchSubscription subscribePatches(RDFChanges target) {
        return linkGrpc.subscribePatches(target);
    }
}
