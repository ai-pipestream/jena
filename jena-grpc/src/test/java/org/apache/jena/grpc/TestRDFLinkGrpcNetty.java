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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.jena.graph.Node;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.JenaTransactionException;
import org.apache.jena.rdflink.AbstractTestRDFLink;
import org.apache.jena.rdflink.RDFLink;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.DatasetGraphWrapper;
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

    private static void awaitCount(IntSupplier counter, int expected, long millis) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        while ( counter.getAsInt() != expected && System.nanoTime() < deadline )
            Thread.sleep(10);
        assertEquals(expected, counter.getAsInt());
    }

    // The in-process server runs handlers on the caller's thread, so a client call blocks for
    // as long as the server does. The transaction command timeout can only be exercised over
    // a transport that dispatches on its own executor, such as Netty.

    @Test
    public void txnCommandTimeout_beginWriteTimesOutWhileLockIsHeld() {
        try ( RDFLinkGrpc holder = RDFLinkGrpc.connect("127.0.0.1", port);
              RDFLinkGrpc waiter = RDFLinkGrpc.newBuilder()
                  .host("127.0.0.1")
                  .port(port)
                  .txnCommandTimeout(Duration.ofMillis(300))
                  .build() ) {
            holder.begin(ReadWrite.WRITE);
            JenaTransactionException ex = assertThrows(JenaTransactionException.class,
                () -> waiter.begin(ReadWrite.WRITE));
            assertTrue(ex.getMessage().contains("Timed out"), ex.getMessage());
            assertTrue(ex.getMessage().contains("BEGIN"), ex.getMessage());
            assertFalse(waiter.isInTransaction());

            holder.update("INSERT DATA { <http://example/s> <http://example/p> 'held' }");
            holder.commit();
            holder.end();

            // The cancelled BEGIN did not leak a transaction: the waiter can now begin.
            assertTimeout(Duration.ofSeconds(10), () -> waiter.begin(ReadWrite.WRITE));
            waiter.abort();
            waiter.end();
            assertEquals(1, waiter.get().size());
        }
    }

    @Test
    public void txnCommandTimeout_slowCommitReportsUnknownOutcome() throws Exception {
                DatasetGraph slowCommit = new DatasetGraphWrapper(DatasetGraphFactory.createTxnMem()) {
            @Override
            public void commit() {
                try {
                    Thread.sleep(600);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                super.commit();
            }
        };
        try ( RDFGrpcServer slowServer = RDFGrpcServer.newBuilder()
                  .port(0)
                  .dataset(slowCommit)
                  .build()
                  .start();
              RDFLinkGrpc link = RDFLinkGrpc.newBuilder()
                  .host("127.0.0.1")
                  .port(slowServer.getPort())
                  .txnCommandTimeout(Duration.ofMillis(200))
                  .build();
              RDFLinkGrpc other = RDFLinkGrpc.connect("127.0.0.1", slowServer.getPort()) ) {
            link.begin(ReadWrite.WRITE);
            link.update("INSERT DATA { <http://example/s> <http://example/p> 'slow' }");
            JenaTransactionException ex = assertThrows(JenaTransactionException.class, link::commit);
            assertTrue(ex.getMessage().contains("outcome of the commit is unknown"), ex.getMessage());
            assertFalse(link.isInTransaction());

            // The server finished the commit after the client gave up.
            awaitCount(() -> other.get().size(), 1, 5_000);
        }
    }

}
