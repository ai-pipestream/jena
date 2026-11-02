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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryParseException;
import org.apache.jena.rdfconnection.AbstractTestRDFConnection;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfpatch.changes.RDFChangesCollector;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;

/**
 * Tests {@link RDFConnectionGrpc} against {@link AbstractTestRDFConnection}.
 */
public class TestRDFConnectionGrpc extends AbstractTestRDFConnection {

    private static final String SERVER_PREFIX = "jena-grpc-conn-";
    private static final AtomicInteger SERVER_SEQ = new AtomicInteger();

    private DatasetGraph serverDsg;
    private String serverName;
    private RDFGrpcServer server;

    @BeforeEach
    public void beforeEach() {
        serverDsg = DatasetGraphFactory.createTxnMem();
        serverName = SERVER_PREFIX + SERVER_SEQ.incrementAndGet();
        server = RDFGrpcServer.newBuilder()
            .inProcessName(serverName)
            .dataset(serverDsg)
            .build()
            .start();
    }

    @AfterEach
    public void afterEach() {
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
    protected RDFConnection connection() {
        return RDFConnectionGrpc.connectInProcess(serverName);
    }

    @Test
    public void connectionGrpc_patchAndFindStream() {
        Node s = NodeFactory.createURI("http://example/s");
        Node p = NodeFactory.createURI("http://example/p");
        Node o = NodeFactory.createLiteralString("hello");

        RDFChangesCollector collector = new RDFChangesCollector();
        collector.start();
        collector.txnBegin();
        collector.add(null, s, p, o);
        collector.txnCommit();
        collector.finish();

        try ( RDFConnectionGrpc conn = RDFConnectionGrpc.connectInProcess(serverName) ) {
            conn.applyPatch(collector.getRDFPatch());
            try ( Stream<Quad> quads = conn.findStream(Node.ANY, s, Node.ANY, Node.ANY) ) {
                assertEquals(1, quads.count());
            }
        }
    }

    @Test
    public void connectionGrpc_parseCheckOverrides() {
        try ( RDFConnectionGrpc conn = RDFConnectionGrpc.connectInProcess(serverName, true) ) {
            assertThrows(QueryParseException.class, () -> {
                try ( QueryExecution qe = conn.newQuery().query("FOOBAR").build() ) {}
            });
            try ( QueryExecution qe = conn.newQuery().parseCheck(false).query("FOOBAR").build() ) {}
            assertThrows(QueryParseException.class, () -> conn.newUpdate().update("FOOBAR"));
            conn.newUpdate().parseCheck(false).update("FOOBAR").build();
        }
    }
}
