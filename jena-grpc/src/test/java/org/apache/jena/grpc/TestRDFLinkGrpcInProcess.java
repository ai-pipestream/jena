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
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.QueryException;
import org.apache.jena.query.QueryParseException;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdflink.AbstractTestRDFLink;
import org.apache.jena.rdflink.RDFLink;
import org.apache.jena.rdfpatch.RDFPatch;
import org.apache.jena.rdfpatch.changes.RDFChangesApply;
import org.apache.jena.rdfpatch.changes.RDFChangesBase;
import org.apache.jena.rdfpatch.changes.RDFChangesCollector;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.riot.system.StreamRDFOps;
import org.apache.jena.sparql.JenaTransactionException;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.exec.QueryExec;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.sparql.util.IsoMatcher;
import org.apache.jena.system.Txn;
import org.apache.jena.tdb2.DatabaseMgr;

/**
 * Tests {@link RDFLinkGrpc} and {@link RDFDatasetGrpcService} over the in-process gRPC transport.
 */
public class TestRDFLinkGrpcInProcess extends AbstractTestRDFLink {

    private static final String SERVER_PREFIX = "jena-grpc-inproc-";
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
    protected RDFLink link() {
        return RDFLinkGrpc.connectInProcess(serverName);
    }

    @Test
    public void findStream_wildcardsAndSpecificPatterns() {
        DatasetGraph source = SSE.parseDatasetGraph(
            "(dataset (graph (:s1 :p1 :o1) (:s2 :p1 :o2)) (graph :g1 (:s1 :p1 :o3) (:s3 :p2 :o4)))");
        try ( RDFLinkGrpc link = RDFLinkGrpc.connectInProcess(serverName) ) {
            link.putDataset(source);

            try ( Stream<Quad> allQuads = link.findStream(Node.ANY, Node.ANY, Node.ANY, Node.ANY) ) {
                List<Quad> list = allQuads.toList();
                assertEquals(4, list.size());
            }

            Node p1 = SSE.parseNode(":p1");
            try ( Stream<Quad> p1Quads = link.findStream(Node.ANY, Node.ANY, p1, Node.ANY) ) {
                assertEquals(3, p1Quads.count());
            }

            Node g1 = SSE.parseNode(":g1");
            try ( Stream<Quad> g1Quads = link.findStream(g1, Node.ANY, Node.ANY, Node.ANY) ) {
                assertEquals(2, g1Quads.count());
            }

            DatasetGraph collected = DatasetGraphFactory.createTxnMem();
            link.findStreamBatched(Node.ANY, Node.ANY, Node.ANY, Node.ANY, StreamRDFLib.dataset(collected));
            assertTrue(IsoMatcher.isomorphic(source, collected));
        }
    }

    @Test
    public void streamLoadDataset_andBatchedIngest() {
        DatasetGraph source = SSE.parseDatasetGraph(
            "(dataset (graph (:s1 :p1 100) (:s2 :p2 'text'@en--ltr)) (graph :g1 (<<( :a :b :c )>> :p3 3.14)))");
        try ( RDFLinkGrpc link = RDFLinkGrpc.connectInProcess(serverName) ) {
            link.streamLoadDatasetBatched(stream -> StreamRDFOps.sendDatasetToStream(source, stream), 2);
            DatasetGraph loaded = link.getDataset();
            assertTrue(IsoMatcher.isomorphic(source, loaded));
        }
    }

    @Test
    public void applyPatch_andSubscribePatches() throws InterruptedException {
        Node g1 = NodeFactory.createURI("http://example/g1");
        Node s = NodeFactory.createURI("http://example/s");
        Node p = NodeFactory.createURI("http://example/p");
        Node o1 = NodeFactory.createLiteralString("v1");
        Node o2 = NodeFactory.createLiteralString("v2");

        RDFChangesCollector collector = new RDFChangesCollector();
        collector.start();
        collector.txnBegin();
        collector.addPrefix(null, "ex", "http://example/");
        collector.add(null, s, p, o1);
        collector.add(g1, s, p, o2);
        collector.txnCommit();
        collector.finish();
        RDFPatch patch = collector.getRDFPatch();

        CountDownLatch commitLatch = new CountDownLatch(1);
        DatasetGraph replicaDsg = DatasetGraphFactory.createTxnMem();
        RDFChangesCollector receivedCollector = new RDFChangesCollector();

        try ( RDFLinkGrpc link = RDFLinkGrpc.connectInProcess(serverName);
              RDFLinkGrpc.PatchSubscription sub = link.subscribePatches(new RDFChangesBase() {
                  @Override
                  public void addPrefix(Node gn, String prefix, String uriStr) {
                      receivedCollector.addPrefix(gn, prefix, uriStr);
                  }

                  @Override
                  public void add(Node g, Node subj, Node pred, Node obj) {
                      receivedCollector.add(g, subj, pred, obj);
                  }

                  @Override
                  public void txnBegin() {
                      receivedCollector.txnBegin();
                  }

                  @Override
                  public void txnCommit() {
                      receivedCollector.txnCommit();
                      commitLatch.countDown();
                  }
              }) ) {

            link.applyPatch(patch);
            assertTrue(commitLatch.await(5, TimeUnit.SECONDS));

            receivedCollector.getRDFPatch().apply(new RDFChangesApply(replicaDsg));
            DatasetGraph serverCopy = link.getDataset();
            assertFalse(serverCopy.isEmpty());
            assertTrue(IsoMatcher.isomorphic(serverCopy, replicaDsg));
        }
    }

    @Test
    public void applyPatchBatch_commitsAndDeletes() {
        Node s = NodeFactory.createURI("http://example/s");
        Node p = NodeFactory.createURI("http://example/p");
        Node o1 = NodeFactory.createLiteralString("v1");
        Node o2 = NodeFactory.createLiteralString("v2");

        try ( RDFLinkGrpc link = RDFLinkGrpc.connectInProcess(serverName) ) {
            RDFChangesCollector addCollector = new RDFChangesCollector();
            addCollector.start();
            addCollector.txnBegin();
            addCollector.add(null, s, p, o1);
            addCollector.add(null, s, p, o2);
            addCollector.txnCommit();
            addCollector.finish();
            link.applyPatchBatch(addCollector.getRDFPatch());
            assertEquals(2, link.get().size());

            RDFChangesCollector delCollector = new RDFChangesCollector();
            delCollector.start();
            delCollector.txnBegin();
            delCollector.delete(null, s, p, o1);
            delCollector.txnCommit();
            delCollector.finish();
            link.applyPatchBatch(delCollector.getRDFPatch());
            assertEquals(1, link.get().size());
        }
    }

    @Test
    public void nonStandardSyntax_parseCheckFlag() {
        try ( RDFLinkGrpc checked = RDFLinkGrpc.newBuilder().inProcessName(serverName).parseCheckSPARQL(true).build() ) {
            assertThrows(QueryParseException.class, () -> checked.query("FOOBAR"));
        }
        try ( RDFLinkGrpc unchecked = RDFLinkGrpc.newBuilder().inProcessName(serverName).parseCheckSPARQL(false).build() ) {
            try ( QueryExec qExec = unchecked.query("FOOBAR") ) {
                assertThrows(QueryException.class, qExec::select);
            }
        }
    }

    @Test
    public void tdb2BackedServer_transactionalOperations() throws Exception {
        DatasetGraph tdb2Dsg = DatabaseMgr.createDatasetGraph();
        String tdbServerName = "jena-grpc-tdb2-" + SERVER_SEQ.incrementAndGet();
        try ( RDFGrpcServer tdbServer = RDFGrpcServer.newBuilder().inProcessName(tdbServerName).dataset(tdb2Dsg).build();
              RDFLinkGrpc link = RDFLinkGrpc.connectInProcess(tdbServerName) ) {
            tdbServer.start();
            Node g1 = SSE.parseNode(":g1");
            Node s = SSE.parseNode(":s");
            Node p = SSE.parseNode(":p");
            Node o = SSE.parseNode("'tdb2-value'");

            Txn.executeWrite(link, () -> link.streamLoadDataset(stream -> stream.quad(Quad.create(g1, s, p, o))));
            Txn.executeRead(link, () -> {
                try ( Stream<Quad> quads = link.findStream(g1, null, null, null) ) {
                    assertEquals(1, quads.count());
                }
                assertTrue(link.queryAsk("ASK { GRAPH <http://example/g1> { ?s ?p 'tdb2-value' } }"));
            });
        }
    }

    private static RDFPatch addPatch(Node s, Node p, Node o) {
        RDFChangesCollector collector = new RDFChangesCollector();
        collector.start();
        collector.txnBegin();
        collector.add(null, s, p, o);
        collector.txnCommit();
        collector.finish();
        return collector.getRDFPatch();
    }

    @Test
    public void subscribePatches_onlyCommittedTxnPatches() throws InterruptedException {
        Node s = NodeFactory.createURI("http://example/s");
        Node p = NodeFactory.createURI("http://example/p");
        Node aborted = NodeFactory.createLiteralString("aborted");
        Node pending = NodeFactory.createLiteralString("pending");
        Node committed = NodeFactory.createLiteralString("committed");

        List<Node> received = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch committedLatch = new CountDownLatch(1);

        try ( RDFLinkGrpc link = RDFLinkGrpc.connectInProcess(serverName);
              RDFLinkGrpc.PatchSubscription sub = link.subscribePatches(new RDFChangesBase() {
                  @Override
                  public void add(Node g, Node subj, Node pred, Node obj) {
                      received.add(obj);
                      if ( obj.equals(committed) )
                          committedLatch.countDown();
                  }
              }) ) {

            link.begin(ReadWrite.WRITE);
            link.applyPatch(addPatch(s, p, aborted));
            link.abort();
            link.end();

            link.begin(ReadWrite.WRITE);
            link.applyPatch(addPatch(s, p, pending));
            link.applyPatch(addPatch(s, p, committed));
            link.commit();
            link.end();

            assertTrue(committedLatch.await(5, TimeUnit.SECONDS));
            assertEquals(List.of(pending, committed), List.copyOf(received));
            assertEquals(2, link.get().size());
        }
    }

    @Test
    public void subscribePatches_concurrentPatchesArriveWhole() throws Exception {
        int threads = 8;
        int patchesPerThread = 20;
        int total = threads * patchesPerThread;
        Node p = NodeFactory.createURI("http://example/p");

        List<String> events = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch done = new CountDownLatch(total);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try ( RDFLinkGrpc link = RDFLinkGrpc.connectInProcess(serverName);
              RDFLinkGrpc.PatchSubscription sub = link.subscribePatches(new RDFChangesBase() {
                  @Override
                  public void txnBegin() { events.add("TX"); }

                  @Override
                  public void add(Node g, Node subj, Node pred, Node obj) { events.add("A"); }

                  @Override
                  public void txnCommit() {
                      events.add("TC");
                      done.countDown();
                  }
              }) ) {
            List<Future<?>> futures = new ArrayList<>();
            for ( int t = 0; t < threads; t++ ) {
                int thread = t;
                futures.add(pool.submit(() -> {
                    for ( int i = 0; i < patchesPerThread; i++ ) {
                        Node s = NodeFactory.createURI("http://example/s" + thread + "-" + i);
                        link.applyPatch(addPatch(s, p, NodeFactory.createLiteralString("v")));
                    }
                }));
            }
            for ( Future<?> f : futures )
                f.get(30, TimeUnit.SECONDS);

            assertTrue(done.await(10, TimeUnit.SECONDS));
            List<String> seen = List.copyOf(events);
            assertEquals(3 * total, seen.size());
            for ( int i = 0; i < seen.size(); i += 3 ) {
                assertEquals(List.of("TX", "A", "TC"), seen.subList(i, i + 3), "Interleaved patch at row " + i);
            }
            assertEquals(total, link.get().size());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    public void idleRemoteTransaction_isAbortedAndReleasesWriteLock() throws Exception {
        String name = "jena-grpc-idle-" + SERVER_SEQ.incrementAndGet();
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
        try ( RDFGrpcServer idleServer = RDFGrpcServer.newBuilder()
                  .inProcessName(name)
                  .dataset(dsg)
                  .txnIdleTimeout(Duration.ofMillis(200))
                  .build()
                  .start();
              RDFLinkGrpc abandoned = RDFLinkGrpc.connectInProcess(name);
              RDFLinkGrpc other = RDFLinkGrpc.connectInProcess(name) ) {

            abandoned.begin(ReadWrite.WRITE);
            abandoned.update("INSERT DATA { <http://example/s> <http://example/p> 'abandoned' }");
            assertEquals(1, idleServer.getService().activeTxnCount());

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while ( idleServer.getService().activeTxnCount() > 0 && System.nanoTime() < deadline )
                Thread.sleep(20);
            assertEquals(0, idleServer.getService().activeTxnCount());

            // The expired transaction no longer holds the writer lock.
            assertTimeoutPreemptively(Duration.ofSeconds(10),
                () -> other.update("INSERT DATA { <http://example/s> <http://example/p> 'other' }"));

            assertThrows(JenaTransactionException.class, abandoned::commit);
            abandoned.end();
            assertEquals(1, other.get().size());
        }
    }

    @Test
    public void nullAndInvalidArguments_throwIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> new RDFDatasetGrpcService(null));
        assertThrows(IllegalArgumentException.class, () -> new RDFDatasetGrpcService(serverDsg, false, 0));
        assertThrows(IllegalArgumentException.class, () -> new RDFDatasetGrpcService(serverDsg, false, 10, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> RDFGrpcServer.newBuilder().txnIdleTimeout(null));
        assertThrows(IllegalArgumentException.class, () -> RDFGrpcServer.newBuilder().port(-1));
        assertThrows(IllegalArgumentException.class, () -> RDFGrpcServer.newBuilder().dataset(null));
        assertThrows(IllegalArgumentException.class, () -> RDFGrpcServer.newBuilder().inProcessName(" "));
        assertThrows(IllegalArgumentException.class, () -> new RDFConnectionGrpc(null));

        try ( RDFLinkGrpc link = RDFLinkGrpc.connectInProcess(serverName) ) {
            assertThrows(IllegalArgumentException.class, () -> link.findStream(null, null, null, null, null));
            assertThrows(IllegalArgumentException.class, () -> link.findStreamBatched(null, null, null, null, null));
            assertThrows(IllegalArgumentException.class, () -> link.streamLoadDataset(null));
            assertThrows(IllegalArgumentException.class, () -> link.streamLoadDatasetBatched(null, 10));
            assertThrows(IllegalArgumentException.class, () -> link.streamLoadDatasetBatched(s -> {}, 0));
            assertThrows(IllegalArgumentException.class, () -> link.applyPatch((RDFPatch) null));
            assertThrows(IllegalArgumentException.class, () -> link.applyPatchBatch(null));
            assertThrows(IllegalArgumentException.class, () -> link.subscribePatches(null));
        }
    }
}
