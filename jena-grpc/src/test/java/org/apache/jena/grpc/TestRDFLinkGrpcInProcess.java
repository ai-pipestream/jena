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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.grpc.wire.PB_RDFService.ApplyPatchRequest;
import org.apache.jena.grpc.wire.PB_RDFService.ExecuteUpdateRequest;
import org.apache.jena.grpc.wire.PB_RDFService.WriteDisposition;
import org.apache.jena.grpc.wire.PB_RDFService.WriteRdfHeader;
import org.apache.jena.grpc.wire.PB_RDFService.WriteRdfRequest;
import org.apache.jena.query.QueryException;
import org.apache.jena.query.QueryParseException;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.rdflink.AbstractTestRDFLink;
import org.apache.jena.rdflink.RDFLink;
import org.apache.jena.rdfpatch.RDFChanges;
import org.apache.jena.rdfpatch.RDFPatch;
import org.apache.jena.rdfpatch.binary.RDFChangesWriterProtobuf;
import org.apache.jena.rdfpatch.changes.RDFChangesApply;
import org.apache.jena.rdfpatch.changes.RDFChangesBase;
import org.apache.jena.rdfpatch.changes.RDFChangesCollector;
import org.apache.jena.riot.protobuf.ProtobufConvert;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Patch_Row;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_StreamRow;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.riot.system.StreamRDFOps;
import org.apache.jena.sparql.JenaTransactionException;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.DatasetGraphWrapper;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Transactional.Promote;
import org.apache.jena.sparql.exec.QueryExec;
import org.apache.jena.sparql.exec.RowSet;
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
    public void queryExec_abortCancelsActiveSelectStream() {
        DatasetGraph source = SSE.parseDatasetGraph(
            "(dataset (graph (:s1 :p1 :o1) (:s2 :p1 :o2) (:s3 :p1 :o3)))");
        try ( RDFLinkGrpc link = RDFLinkGrpc.connectInProcess(serverName) ) {
            link.putDataset(source);
            try ( QueryExec qExec = link.query("SELECT ?s ?o WHERE { ?s ?p ?o }") ) {
                RowSet rs = qExec.select();
                assertTrue(rs.hasNext());
                rs.next();
                qExec.abort();
                assertTrue(qExec.isClosed());
                assertFalse(rs.hasNext());
            }
        }
    }

    @Test
    public void remoteTransaction_promoteReadToWrite() {
        try ( RDFLinkGrpc link = RDFLinkGrpc.connectInProcess(serverName) ) {
            link.begin(TxnType.READ_PROMOTE);
            assertEquals(ReadWrite.READ, link.transactionMode());
            assertTrue(link.promote(Promote.ISOLATED));
            assertEquals(ReadWrite.WRITE, link.transactionMode());
            link.update("INSERT DATA { <http://example/s> <http://example/p> 'promoted' }");
            link.commit();
            link.end();
            assertEquals(1, link.get().size());
        }
    }

    @Test
    public void abruptTransactionStreamDisconnect_immediatelyAbortsAndReleasesWriteLock() throws Exception {
        String name = "jena-grpc-bidi-abort-" + SERVER_SEQ.incrementAndGet();
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
        try ( RDFGrpcServer longTimeoutServer = RDFGrpcServer.newBuilder()
                  .inProcessName(name)
                  .dataset(dsg)
                  .txnIdleTimeout(Duration.ofMinutes(10))
                  .build()
                  .start();
              RDFLinkGrpc disconnected = RDFLinkGrpc.connectInProcess(name);
              RDFLinkGrpc other = RDFLinkGrpc.connectInProcess(name) ) {

            disconnected.begin(ReadWrite.WRITE);
            disconnected.update("INSERT DATA { <http://example/s> <http://example/p> 'uncommitted' }");
            assertEquals(1, longTimeoutServer.getService().activeTxnCount());

            // Simulate an abrupt client stream disconnect without waiting for the 10-minute idle timeout.
            disconnected.cancelTransactionStreamAbruptly();

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while ( longTimeoutServer.getService().activeTxnCount() > 0 && System.nanoTime() < deadline ) {
                Thread.sleep(10);
            }
            assertEquals(0, longTimeoutServer.getService().activeTxnCount());

            // Another client can immediately acquire the write lock, and uncommitted data was rolled back.
            assertTimeoutPreemptively(Duration.ofSeconds(5),
                () -> other.update("INSERT DATA { <http://example/s> <http://example/p> 'committed' }"));
            assertEquals(1, other.get().size());
        }
    }

    @Test
    public void constructTriples_describeTriples_constructQuads_streamLazily() {
        DatasetGraph source = SSE.parseDatasetGraph(
            "(dataset (graph (:s1 :p1 :o1) (:s2 :p1 :o2) (:s3 :p1 :o3)) (graph :g1 (:s4 :p2 :o4)))");
        try ( RDFLinkGrpc link = RDFLinkGrpc.newBuilder().inProcessName(serverName).batchSize(1).build() ) {
            link.putDataset(source);

            try ( QueryExec qExec = link.query("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }") ) {
                Iterator<Triple> iter = qExec.constructTriples();
                assertTrue(iter.hasNext());
                iter.next();
                qExec.abort();
                assertFalse(iter.hasNext());
            }

            try ( QueryExec qExec = link.query("DESCRIBE <http://example/s1>") ) {
                Iterator<Triple> iter = qExec.describeTriples();
                assertTrue(iter.hasNext());
                assertEquals(SSE.parseNode(":s1"), iter.next().getSubject());
            }

            try ( QueryExec qExec = link.query("CONSTRUCT { GRAPH ?g { ?s ?p ?o } } WHERE { GRAPH ?g { ?s ?p ?o } }") ) {
                Iterator<Quad> iter = qExec.constructQuads();
                assertTrue(iter.hasNext());
                assertEquals(SSE.parseNode(":g1"), iter.next().getGraph());
            }
        }
    }

    @Test
    public void findStream_earlyCloseCancelsServerStream() {
        String name = "jena-grpc-small-batch-" + SERVER_SEQ.incrementAndGet();
        DatasetGraph dsg = SSE.parseDatasetGraph(
            "(dataset (graph (:s1 :p1 :o1) (:s2 :p1 :o2) (:s3 :p1 :o3) (:s4 :p1 :o4)))");
        try ( RDFGrpcServer smallBatchServer = RDFGrpcServer.newBuilder()
                  .inProcessName(name)
                  .dataset(dsg)
                  .batchSize(1)
                  .build()
                  .start();
              RDFLinkGrpc link = RDFLinkGrpc.newBuilder().inProcessName(name).batchSize(1).build() ) {
            try ( Stream<Quad> stream = link.findStream(Node.ANY, Node.ANY, Node.ANY, Node.ANY) ) {
                assertTrue(stream.findFirst().isPresent());
            }
            try ( Stream<Quad> fullStream = link.findStream(Node.ANY, Node.ANY, Node.ANY, Node.ANY) ) {
                assertEquals(4, fullStream.count());
            }
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

    // ---- Round 2 review fixes

    private static final Node EX_S = NodeFactory.createURI("http://example/s");
    private static final Node EX_P = NodeFactory.createURI("http://example/p");
    private static final Node EX_G1 = NodeFactory.createURI("http://example/g1");
    private static final Node EX_G2 = NodeFactory.createURI("http://example/g2");

    private static Node lit(String value) {
        return NodeFactory.createLiteralString(value);
    }

    private static List<RDF_Patch_Row> patchRows(Consumer<RDFChanges> action) {
        List<RDF_Patch_Row> rows = new ArrayList<>();
        RDFChanges writer = RDFChangesWriterProtobuf.create(rows::add);
        writer.start();
        action.accept(writer);
        writer.finish();
        return rows;
    }

    private static RDFPatch patch(Consumer<RDFChanges> action) {
        RDFChangesCollector collector = new RDFChangesCollector();
        collector.start();
        collector.txnBegin();
        action.accept(collector);
        collector.txnCommit();
        collector.finish();
        return collector.getRDFPatch();
    }

    /** Records patch events as strings: "TX", "TC", "A g o", "D g o", "P g prefix". */
    private static final class EventRecorder extends RDFChangesBase {
        final List<String> events = Collections.synchronizedList(new ArrayList<>());
        private final CountDownLatch commits;

        EventRecorder(int expectedCommits) {
            this.commits = new CountDownLatch(expectedCommits);
        }

        private static String g(Node g) {
            return (g == null) ? "default" : g.getURI();
        }

        @Override public void txnBegin() { events.add("TX"); }
        @Override public void add(Node g, Node s, Node p, Node o) { events.add("A " + g(g) + " " + o.getLiteralLexicalForm()); }
        @Override public void delete(Node g, Node s, Node p, Node o) { events.add("D " + g(g) + " " + o.getLiteralLexicalForm()); }
        @Override public void addPrefix(Node g, String prefix, String uri) { events.add("P " + g(g) + " " + prefix); }
        @Override public void deletePrefix(Node g, String prefix) { events.add("PD " + g(g) + " " + prefix); }
        @Override public void txnCommit() { events.add("TC"); commits.countDown(); }

        boolean await() throws InterruptedException {
            return commits.await(5, TimeUnit.SECONDS);
        }
    }

    private static void awaitCount(java.util.function.IntSupplier counter, int expected, long millis) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        while ( counter.getAsInt() != expected && System.nanoTime() < deadline )
            Thread.sleep(10);
        assertEquals(expected, counter.getAsInt());
    }

    /** Client-streaming write that sends {@code frames} and then stalls without half-closing. */
    private static final class StalledCall<Req, Resp> implements AutoCloseable {
        final ManagedChannel channel;
        final StreamObserver<Req> requests;
        final CountDownLatch terminated = new CountDownLatch(1);
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final AtomicReference<Resp> response = new AtomicReference<>();

        StalledCall(String serverName, MethodDescriptor<Req, Resp> method, List<Req> frames) {
            channel = InProcessChannelBuilder.forName(serverName).usePlaintext().build();
            requests = ClientCalls.asyncClientStreamingCall(channel.newCall(method, CallOptions.DEFAULT),
                new StreamObserver<>() {
                    @Override public void onNext(Resp value) { response.set(value); }
                    @Override public void onError(Throwable t) { error.set(t); terminated.countDown(); }
                    @Override public void onCompleted() { terminated.countDown(); }
                });
            frames.forEach(requests::onNext);
        }

        @Override
        public void close() {
            channel.shutdownNow();
        }
    }

    private <Req, Resp> void assertStalledWriteIsReaped(MethodDescriptor<Req, Resp> method, List<Req> frames) throws Exception {
        String name = "jena-grpc-stall-" + SERVER_SEQ.incrementAndGet();
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
        try ( RDFGrpcServer stallServer = RDFGrpcServer.newBuilder()
                  .inProcessName(name)
                  .dataset(dsg)
                  .txnIdleTimeout(Duration.ofMillis(200))
                  .build()
                  .start();
              StalledCall<Req, Resp> stalled = new StalledCall<>(name, method, frames) ) {
            RDFDatasetGrpcService service = stallServer.getService();
            awaitCount(service::ownedWriteCount, 1, 5_000);
            assertEquals(0, service.activeTxnCount());

            // The reaper aborts the stalled session and fails the call.
            awaitCount(service::ownedWriteCount, 0, 10_000);
            assertTrue(stalled.terminated.await(5, TimeUnit.SECONDS));
            StatusRuntimeException sre = assertInstanceOf(StatusRuntimeException.class, stalled.error.get());
            assertEquals(Status.Code.ABORTED, sre.getStatus().getCode());

            // The writer lock is free again and the partial write was rolled back.
            assertTimeoutPreemptively(Duration.ofSeconds(5),
                () -> Txn.executeWrite(dsg, () -> dsg.add(Quad.defaultGraphIRI, EX_S, EX_P, lit("other"))));
            Txn.executeRead(dsg, () -> {
                assertEquals(1, Iter.count(dsg.find()));
                assertFalse(dsg.contains(Node.ANY, EX_S, EX_P, lit("stalled")));
            });
        }
    }

    private Throwable executeUpdateRaw(ExecuteUpdateRequest request) throws Exception {
        try ( StalledCall<ExecuteUpdateRequest, ?> call = new StalledCall<>(serverName,
                  RDFGrpcProtocol.EXECUTE_UPDATE_METHOD, List.of(request)) ) {
            call.requests.onCompleted();
            assertTrue(call.terminated.await(5, TimeUnit.SECONDS));
            return call.error.get();
        }
    }

    @Test
    public void executeUpdate_usingGraphUrisSelectWhereBindings() throws Exception {
        Txn.executeWrite(serverDsg, () -> {
            serverDsg.add(Quad.defaultGraphIRI, EX_S, EX_P, lit("from-default"));
            serverDsg.add(EX_G1, EX_S, EX_P, lit("from-g1"));
        });
        Node copy = NodeFactory.createURI("http://example/copy");
        Throwable error = executeUpdateRaw(ExecuteUpdateRequest.newBuilder()
            .setUpdate("INSERT { <http://example/copy> <http://example/p> ?o } "
                + "WHERE { ?s <http://example/p> ?o }")
            .addUsingGraphUris("http://example/g1")
            .build());
        assertEquals(null, error);
        Txn.executeRead(serverDsg, () -> {
            assertTrue(serverDsg.contains(Quad.defaultGraphIRI, copy, EX_P, lit("from-g1")));
            assertFalse(serverDsg.contains(Quad.defaultGraphIRI, copy, EX_P, lit("from-default")));
        });
    }

    @Test
    public void executeUpdate_usingNamedGraphUrisRestrictNamedWhereBindings() throws Exception {
        Txn.executeWrite(serverDsg, () -> {
            serverDsg.add(EX_G1, EX_S, EX_P, lit("from-g1"));
            serverDsg.add(EX_G2, EX_S, EX_P, lit("from-g2"));
        });
        Node copy = NodeFactory.createURI("http://example/copy");
        Throwable error = executeUpdateRaw(ExecuteUpdateRequest.newBuilder()
            .setUpdate("INSERT { <http://example/copy> <http://example/p> ?o } "
                + "WHERE { GRAPH ?g { ?s <http://example/p> ?o } }")
            .addUsingNamedGraphUris("http://example/g1")
            .build());
        assertEquals(null, error);
        Txn.executeRead(serverDsg, () -> {
            assertTrue(serverDsg.contains(Quad.defaultGraphIRI, copy, EX_P, lit("from-g1")));
            assertFalse(serverDsg.contains(Quad.defaultGraphIRI, copy, EX_P, lit("from-g2")));
        });
    }

    @Test
    public void executeUpdate_usingFieldsRejectInlineWith() throws Exception {
        Throwable error = executeUpdateRaw(ExecuteUpdateRequest.newBuilder()
            .setUpdate("WITH <http://example/g2> "
                + "INSERT { <http://example/copy> <http://example/p> 'x' } WHERE {}")
            .addUsingGraphUris("http://example/g1")
            .build());
        StatusRuntimeException sre = assertInstanceOf(StatusRuntimeException.class, error);
        assertEquals(Status.Code.INVALID_ARGUMENT, sre.getStatus().getCode());
        assertFalse(serverDsg.contains(Node.ANY, Node.ANY, Node.ANY, Node.ANY));
    }

    @Test
    public void stalledWriteRdf_isReapedAndReleasesWriteLock() throws Exception {
        Quad quad = Quad.create(Quad.defaultGraphIRI, EX_S, EX_P, lit("stalled"));
        assertStalledWriteIsReaped(RDFGrpcProtocol.WRITE_RDF_METHOD, List.of(
            WriteRdfRequest.newBuilder()
                .setHeader(WriteRdfHeader.newBuilder().setDisposition(WriteDisposition.WRITE_DISPOSITION_APPEND))
                .build(),
            WriteRdfRequest.newBuilder()
                .setRow(RDF_StreamRow.newBuilder().setQuad(ProtobufConvert.convert(quad, false)))
                .build()));
    }

    @Test
    public void stalledExecuteUpdate_isReapedAndReleasesWriteLock() throws Exception {
        assertStalledWriteIsReaped(RDFGrpcProtocol.EXECUTE_UPDATE_METHOD, List.of(
            ExecuteUpdateRequest.newBuilder()
                .setUpdate("INSERT DATA { <http://example/s> <http://example/p> 'stalled' }")
                .build()));
    }

    @Test
    public void stalledApplyPatch_isReapedAndReleasesWriteLock() throws Exception {
        List<RDF_Patch_Row> rows = patchRows(c -> {
            c.txnBegin();
            c.add(null, EX_S, EX_P, lit("stalled"));
        });
        assertStalledWriteIsReaped(RDFGrpcProtocol.APPLY_PATCH_METHOD, List.of(
            ApplyPatchRequest.newBuilder().addAllRows(rows).build()));
    }

    @Test
    public void applyPatch_subscriberJoiningMidStreamGetsNoPartialPatch() throws Exception {
        List<RDF_Patch_Row> chunk1 = patchRows(c -> {
            c.txnBegin();
            c.add(null, EX_S, EX_P, lit("o1"));
        });
        List<RDF_Patch_Row> chunk2 = patchRows(c -> {
            c.add(null, EX_S, EX_P, lit("o2"));
            c.txnCommit();
        });
        EventRecorder recorder = new EventRecorder(1);
        try ( StalledCall<ApplyPatchRequest, ?> inFlight = new StalledCall<>(serverName,
                  RDFGrpcProtocol.APPLY_PATCH_METHOD,
                  List.of(ApplyPatchRequest.newBuilder().addAllRows(chunk1).build()));
              RDFLinkGrpc link = RDFLinkGrpc.connectInProcess(serverName) ) {
            awaitCount(server.getService()::ownedWriteCount, 1, 5_000);
            try ( RDFLinkGrpc.PatchSubscription sub = link.subscribePatches(recorder) ) {
                inFlight.requests.onNext(ApplyPatchRequest.newBuilder().addAllRows(chunk2).build());
                inFlight.requests.onCompleted();
                assertTrue(inFlight.terminated.await(5, TimeUnit.SECONDS));
                assertEquals(null, inFlight.error.get());

                // The next whole patch is the first thing the subscriber sees.
                link.applyPatch(addPatch(EX_S, EX_P, lit("o3")));
                assertTrue(recorder.await());
                assertEquals(List.of("TX", "A default o3", "TC"), List.copyOf(recorder.events));
                assertEquals(3, link.get().size());
            }
        }
    }

    @Test
    public void subscribePatches_graphFilterDeliversOnlyMatchingRows() throws Exception {
        EventRecorder all = new EventRecorder(2);
        EventRecorder g1Only = new EventRecorder(1);
        EventRecorder defaultOnly = new EventRecorder(2);
        try ( RDFLinkGrpc link = RDFLinkGrpc.connectInProcess(serverName);
              RDFLinkGrpc.PatchSubscription subAll = link.subscribePatches(all);
              RDFLinkGrpc.PatchSubscription subG1 = link.subscribePatchesForGraph(EX_G1, g1Only);
              RDFLinkGrpc.PatchSubscription subDefault = link.subscribePatchesForGraph(null, defaultOnly) ) {

            // Touches only the default graph: not delivered to the g1 subscriber at all.
            link.applyPatch(patch(c -> c.add(null, EX_S, EX_P, lit("o1"))));
            // Touches the default graph and two named graphs.
            link.applyPatch(patch(c -> {
                c.add(null, EX_S, EX_P, lit("o2"));
                c.add(EX_G1, EX_S, EX_P, lit("o3"));
                c.addPrefix(EX_G1, "ex", "http://example/");
                c.add(EX_G2, EX_S, EX_P, lit("o4"));
            }));

            assertTrue(all.await());
            assertTrue(g1Only.await());
            assertTrue(defaultOnly.await());
            assertEquals(List.of(
                    "TX", "A default o1", "TC",
                    "TX", "A default o2", "A http://example/g1 o3", "P http://example/g1 ex", "A http://example/g2 o4", "TC"),
                List.copyOf(all.events));
            assertEquals(List.of("TX", "A http://example/g1 o3", "P http://example/g1 ex", "TC"),
                List.copyOf(g1Only.events));
            assertEquals(List.of("TX", "A default o1", "TC", "TX", "A default o2", "TC"),
                List.copyOf(defaultOnly.events));
        }
    }

    @Test
    public void subscribePatches_receivesWriteRdfAndUpdateCommits() throws Exception {
        DatasetGraph source = SSE.parseDatasetGraph("(dataset (graph (:s :p 'o1')) (graph :g1 (:s :p 'o2')))");
        source.prefixes().add("ex", "http://example/");
        EventRecorder recorder = new EventRecorder(4);
        DatasetGraph replica = DatasetGraphFactory.createTxnMem();
        RDFChangesCollector received = new RDFChangesCollector();
        try ( RDFLinkGrpc link = RDFLinkGrpc.connectInProcess(serverName);
              RDFLinkGrpc.PatchSubscription sub = link.subscribePatches(new RDFChangesBase() {
                  @Override public void txnBegin() { received.txnBegin(); recorder.txnBegin(); }
                  @Override public void txnCommit() { received.txnCommit(); recorder.txnCommit(); }
                  @Override public void add(Node g, Node s, Node p, Node o) { received.add(g, s, p, o); recorder.add(g, s, p, o); }
                  @Override public void delete(Node g, Node s, Node p, Node o) { received.delete(g, s, p, o); recorder.delete(g, s, p, o); }
                  @Override public void addPrefix(Node g, String prefix, String uri) { received.addPrefix(g, prefix, uri); recorder.addPrefix(g, prefix, uri); }
              }) ) {
            link.putDataset(source);                                                        // WriteRdf, dataset
            link.put(EX_G2, SSE.parseGraph("(graph (:s :p 'o3'))"));          // WriteRdf, graph
            link.update("INSERT DATA { <http://example/s> <http://example/p> 'o4' }");      // ExecuteUpdate
            link.update("DELETE DATA { <http://example/s> <http://example/p> 'o1' }");      // ExecuteUpdate
            assertTrue(recorder.await());

            assertTrue(recorder.events.contains("P default ex"));
            assertEquals(List.of("TX", "A default o4", "TC", "TX", "D default o1", "TC"),
                recorder.events.subList(recorder.events.size() - 6, recorder.events.size()));

            received.getRDFPatch().apply(new RDFChangesApply(replica));
            assertTrue(IsoMatcher.isomorphic(link.getDataset(), replica));
        }
    }

    @Test
    public void subscribePatches_subscriberJoiningDuringRemoteTxnGetsNextCommitOnly() throws Exception {
        EventRecorder recorder = new EventRecorder(1);
        try ( RDFLinkGrpc writer = RDFLinkGrpc.connectInProcess(serverName);
              RDFLinkGrpc reader = RDFLinkGrpc.connectInProcess(serverName) ) {
            writer.begin(ReadWrite.WRITE);
            writer.update("INSERT DATA { <http://example/s> <http://example/p> 'o1' }");
            try ( RDFLinkGrpc.PatchSubscription sub = reader.subscribePatches(recorder) ) {
                writer.update("INSERT DATA { <http://example/s> <http://example/p> 'o2' }");
                writer.commit();
                writer.end();
                writer.update("INSERT DATA { <http://example/s> <http://example/p> 'o3' }");
                assertTrue(recorder.await());
                assertEquals(List.of("TX", "A default o3", "TC"), List.copyOf(recorder.events));
            }
        }
    }

    @Test
    public void txnCommandTimeout_rejectsNonPositive() {
        assertThrows(IllegalArgumentException.class, () -> RDFLinkGrpc.newBuilder().txnCommandTimeout(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> RDFLinkGrpc.newBuilder().txnCommandTimeout(Duration.ofSeconds(-1)));
    }
}
