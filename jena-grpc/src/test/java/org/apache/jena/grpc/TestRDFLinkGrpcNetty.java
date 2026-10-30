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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.jena.graph.Node;
import org.apache.jena.rdflink.AbstractTestRDFLink;
import org.apache.jena.rdflink.RDFLink;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.sparql.util.IsoMatcher;
import org.apache.jena.system.Txn;
import org.apache.jena.tdb2.DatabaseMgr;

/**
 * Tests {@link RDFLinkGrpc} and {@link RDFGrpcServer} over the Netty TCP transport
 * backed by a {@code TDB2} {@link DatasetGraph}.
 */
public class TestRDFLinkGrpcNetty extends AbstractTestRDFLink {

    private static DatasetGraph serverDsg;
    private static RDFGrpcServer server;
    private static int port;

    @BeforeAll
    public static void beforeAll() {
        serverDsg = DatabaseMgr.createDatasetGraph();
        server = RDFGrpcServer.newBuilder()
            .port(0)
            .dataset(serverDsg)
            .build()
            .start();
        port = server.getPort();
    }

    @BeforeEach
    public void beforeEach() {
        Txn.executeWrite(serverDsg, serverDsg::clear);
    }

    @AfterAll
    public static void afterAll() {
        if ( server != null ) {
            server.close();
            server = null;
        }
    }

    @Override
    protected boolean supportsAbort() {
        return true;
    }

    @Override
    protected RDFLink link() {
        return RDFLinkGrpc.connect("127.0.0.1", port);
    }

    @Test
    public void tdb2Netty_findStreamAndPersistence() {
        DatasetGraph sample = SSE.parseDatasetGraph(
            "(dataset (graph (:s1 :p1 :o1)) (graph :g1 (:s2 :p2 42)))");
        try ( RDFLinkGrpc link = RDFLinkGrpc.connect("127.0.0.1", port) ) {
            link.putDataset(sample);
            try ( Stream<Quad> quads = link.findStream(Node.ANY, Node.ANY, Node.ANY, Node.ANY) ) {
                assertEquals(2, quads.count());
            }
            DatasetGraph fetched = link.getDataset();
            assertTrue(IsoMatcher.isomorphic(sample, fetched));
        }
    }
}
