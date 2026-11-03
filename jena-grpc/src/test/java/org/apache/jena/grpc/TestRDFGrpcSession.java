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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.grpc.wire.PB_RDFService.PutRdfResponse;
import org.apache.jena.grpc.wire.PB_RDFService.StreamRdfRequest;
import org.apache.jena.grpc.wire.PB_RDFService.WriteDisposition;
import org.apache.jena.grpc.wire.PB_RDFService.WriteRdfHeader;
import org.apache.jena.grpc.wire.PB_RDFService.WriteRdfResponse;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.riot.system.StreamRDFOps;
import org.apache.jena.sparql.ARQException;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.sparql.util.IsoMatcher;
import org.apache.jena.system.Txn;

/**
 * Tests the unary calls ({@link RDFGrpcUnary}) and the bidirectional session
 * ({@link RDFGrpcSession}) against {@link RDFDatasetGrpcService}, mostly over the
 * in-process transport, with one round trip and the message size limit over Netty.
 */
public class TestRDFGrpcSession {

    private static final AtomicInteger SERVER_SEQ = new AtomicInteger();

    private DatasetGraph serverDsg;
    private RDFGrpcServer server;
    private ManagedChannel channel;

    @BeforeEach
    public void beforeEach() {
        serverDsg = DatasetGraphFactory.createTxnMem();
        String name = "jena-grpc-session-" + SERVER_SEQ.incrementAndGet();
        server = RDFGrpcServer.newBuilder().inProcessName(name).dataset(serverDsg).build().start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
    }

    @AfterEach
    public void afterEach() {
        channel.shutdownNow();
        server.close();
    }

    private static DatasetGraph sample() {
        return SSE.parseDatasetGraph("""
            (dataset
              (graph (:s :p :o) (:s :p 1) (:s :q "text"@en))
              (graph :g (:s :p :o) (:x :y :z)))
            """);
    }

    private static Graph sampleGraph() {
        return SSE.parseGraph("(graph (:a :b :c) (:a :b 2) (:a :d \"x\"))");
    }

    private static WriteRdfHeader datasetHeader() {
        return WriteRdfHeader.newBuilder().setEntireDataset(true).build();
    }

    private static WriteRdfHeader graphHeader(String graphName) {
        return WriteRdfHeader.newBuilder()
            .setGraph(RDFGrpcProtocol.encodeGraphTarget(NodeFactory.createURI(graphName)))
            .build();
    }

    private static StreamRdfRequest entireDataset() {
        return StreamRdfRequest.newBuilder().setEntireDataset(true).build();
    }

    private DatasetGraph readAll(RDFGrpcSession session) throws Exception {
        DatasetGraph dsg = DatasetGraphFactory.create();
        session.read(entireDataset(), StreamRDFLib.dataset(dsg)).get(10, TimeUnit.SECONDS);
        return dsg;
    }

    private static void assertIso(DatasetGraph expected, DatasetGraph actual) {
        assertTrue(IsoMatcher.isomorphic(expected, actual), "Datasets differ:\n" + expected + "\n" + actual);
    }

    @Test
    public void unary_putThenGet() {
        DatasetGraph data = sample();
        PutRdfResponse put = RDFGrpcUnary.putRdf(channel, datasetHeader(),
            s -> StreamRDFOps.sendDatasetToStream(data, s), true, true);
        assertEquals(5, put.getQuadCount());

        DatasetGraph got = DatasetGraphFactory.create();
        RDFGrpcUnary.getRdf(channel, entireDataset(), StreamRDFLib.dataset(got));
        assertIso(data, got);
        Txn.executeRead(serverDsg, () -> assertIso(data, serverDsg));
    }

    @Test
    public void unary_getGraphWithoutTermTables() {
        Graph g = sampleGraph();
        RDFGrpcUnary.putRdf(channel, graphHeader("http://example/g1"),
            s -> StreamRDFOps.sendGraphToStream(g, s), false, false);
        Graph got = org.apache.jena.graph.GraphMemFactory.createDefaultGraph();
        StreamRdfRequest request = StreamRdfRequest.newBuilder()
            .setGraph(RDFGrpcProtocol.encodeGraphTarget(NodeFactory.createURI("http://example/g1")))
            .build();
        RDFGrpcUnary.getRdf(channel, request, StreamRDFLib.graph(got));
        assertTrue(g.isIsomorphicWith(got));
    }

    @Test
    public void unary_replaceAndClear() {
        DatasetGraph data = sample();
        RDFGrpcUnary.putRdf(channel, datasetHeader(), s -> StreamRDFOps.sendDatasetToStream(data, s), true, true);
        WriteRdfHeader replace = WriteRdfHeader.newBuilder()
            .setEntireDataset(true)
            .setDisposition(WriteDisposition.WRITE_DISPOSITION_REPLACE)
            .build();
        Graph g = sampleGraph();
        PutRdfResponse r = RDFGrpcUnary.putRdf(channel, replace, s -> StreamRDFOps.sendGraphToStream(g, s), true, true);
        assertEquals(3, r.getQuadCount());
        Txn.executeRead(serverDsg, () -> {
            assertEquals(3, serverDsg.getDefaultGraph().size());
            assertFalse(serverDsg.listGraphNodes().hasNext());
        });
        WriteRdfHeader clear = WriteRdfHeader.newBuilder()
            .setEntireDataset(true)
            .setDisposition(WriteDisposition.WRITE_DISPOSITION_CLEAR)
            .build();
        RDFGrpcUnary.putRdf(channel, clear, null, true, true);
        Txn.executeRead(serverDsg, () -> assertTrue(serverDsg.isEmpty()));
    }

    @Test
    public void session_writeThenRead() throws Exception {
        DatasetGraph data = sample();
        try ( RDFGrpcSession session = RDFGrpcSession.open(channel) ) {
            WriteRdfResponse w = session.write(datasetHeader(), s -> StreamRDFOps.sendDatasetToStream(data, s))
                .get(10, TimeUnit.SECONDS);
            assertEquals(5, w.getQuadCount());
            assertIso(data, readAll(session));
        }
        Txn.executeRead(serverDsg, () -> assertIso(data, serverDsg));
    }

    @Test
    public void session_pipelinedWritesAndReads() throws Exception {
        try ( RDFGrpcSession session = RDFGrpcSession.open(channel) ) {
            // The second write starts before the first one's result has arrived.
            RDFGrpcSession.Writer w1 = session.writer(graphHeader("http://example/g1"));
            w1.start();
            for ( int i = 0; i < 100; i++ )
                w1.triple(SSE.parseTriple("(:s :p " + i + ")"));
            w1.finish();
            RDFGrpcSession.Writer w2 = session.writer(graphHeader("http://example/g2"));
            w2.start();
            for ( int i = 0; i < 100; i++ )
                w2.triple(SSE.parseTriple("(:s :q " + i + ")"));
            w2.finish();
            assertEquals(100, w1.result().get(10, TimeUnit.SECONDS).getQuadCount());
            assertEquals(100, w2.result().get(10, TimeUnit.SECONDS).getQuadCount());

            // Two reads in flight before either completes; each is its own encoded stream.
            List<Quad> q1 = new ArrayList<>();
            List<Quad> q2 = new ArrayList<>();
            CompletableFuture<Void> r1 = session.read(StreamRdfRequest.newBuilder()
                .setQuadPattern(RDFGrpcProtocol.encodeQuadPattern(NodeFactory.createURI("http://example/g1"), null, null, null))
                .build(), StreamRDFLib.sinkQuads(new org.apache.jena.atlas.lib.SinkToCollection<>(q1)));
            CompletableFuture<Void> r2 = session.read(entireDataset(),
                StreamRDFLib.sinkQuads(new org.apache.jena.atlas.lib.SinkToCollection<>(q2)));
            r1.get(10, TimeUnit.SECONDS);
            r2.get(10, TimeUnit.SECONDS);
            assertEquals(100, q1.size());
            assertEquals(200, q2.size());
        }
    }

    @Test
    public void session_secondWriteWhileOneIsOpen_isRejected() throws Exception {
        try ( RDFGrpcSession session = RDFGrpcSession.open(channel) ) {
            RDFGrpcSession.Writer w1 = session.writer(graphHeader("http://example/g1"));
            w1.start();
            RDFGrpcSession.Writer w2 = session.writer(graphHeader("http://example/g2"));
            assertThrows(ARQException.class, w2::start);
            w1.finish();
            assertEquals(0, w1.result().get(10, TimeUnit.SECONDS).getQuadCount());
        }
    }

    @Test
    public void session_manySmallWrites() throws Exception {
        try ( RDFGrpcSession session = RDFGrpcSession.open(channel) ) {
            List<CompletableFuture<WriteRdfResponse>> results = new ArrayList<>();
            for ( int d = 0; d < 50; d++ ) {
                final int doc = d;
                results.add(session.write(graphHeader("http://example/doc" + doc), s -> {
                    for ( int i = 0; i < 10; i++ )
                        s.triple(SSE.parseTriple("(:doc" + doc + " :p " + i + ")"));
                }));
            }
            for ( CompletableFuture<WriteRdfResponse> r : results )
                assertEquals(10, r.get(10, TimeUnit.SECONDS).getQuadCount());
        }
        Txn.executeRead(serverDsg, () -> assertEquals(500, Txn.calculateRead(serverDsg, () -> (long) serverDsg.stream().count())));
    }

    @Test
    public void session_withoutTermTables() throws Exception {
        DatasetGraph data = sample();
        try ( RDFGrpcSession session = RDFGrpcSession.open(channel, io.grpc.CallOptions.DEFAULT, false, false, 2) ) {
            session.write(datasetHeader(), s -> StreamRDFOps.sendDatasetToStream(data, s)).get(10, TimeUnit.SECONDS);
            assertIso(data, readAll(session));
        }
    }

    @Test
    public void session_writeStillOpenAtClose_isRolledBack() {
        RDFGrpcSession session = RDFGrpcSession.open(channel);
        RDFGrpcSession.Writer w = session.writer(datasetHeader());
        w.start();
        w.triple(SSE.parseTriple("(:s :p :o)"));
        assertThrows(ARQException.class, session::close);
        assertTrue(w.result().isCompletedExceptionally());
        assertFalse(session.isOpen());
        Txn.executeRead(serverDsg, () -> assertTrue(serverDsg.isEmpty()));
    }

    @Test
    public void session_endOfUnknownWrite_failsSession() {
        RDFGrpcSession session = RDFGrpcSession.open(channel);
        RDFGrpcSession.Writer w = session.writer(datasetHeader());
        w.finish();   // write_end without a write: the server rejects it and the stream fails
        assertThrows(ARQException.class, session::close);
        assertThrows(ARQException.class, () -> session.read(entireDataset(), StreamRDFLib.sinkNull()));
    }

    @Test
    public void session_cancel() {
        RDFGrpcSession session = RDFGrpcSession.open(channel);
        RDFGrpcSession.Writer w = session.writer(datasetHeader());
        w.start();
        w.triple(SSE.parseTriple("(:s :p :o)"));
        session.cancel();
        java.util.concurrent.ExecutionException ex =
            assertThrows(java.util.concurrent.ExecutionException.class, () -> w.result().get(10, TimeUnit.SECONDS));
        assertTrue(ex.getCause() instanceof ARQException);
        Txn.executeRead(serverDsg, () -> assertTrue(serverDsg.isEmpty()));
        assertEquals(0, server.getService().ownedWriteCount());
    }

    @Test
    public void netty_sessionAndUnaryRoundTrip() throws Exception {
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
        try ( RDFGrpcServer nettyServer = RDFGrpcServer.newBuilder().port(0).dataset(dsg).build().start() ) {
            ManagedChannel netty = NettyChannelBuilder.forAddress("127.0.0.1", nettyServer.getPort()).usePlaintext().build();
            try {
                DatasetGraph data = sample();
                try ( RDFGrpcSession session = RDFGrpcSession.open(netty) ) {
                    session.write(datasetHeader(), s -> StreamRDFOps.sendDatasetToStream(data, s)).get(10, TimeUnit.SECONDS);
                    assertIso(data, readAll(session));
                }
                DatasetGraph got = DatasetGraphFactory.create();
                RDFGrpcUnary.getRdf(netty, entireDataset(), StreamRDFLib.dataset(got));
                assertIso(data, got);
            } finally {
                netty.shutdownNow();
            }
        }
    }

    @Test
    public void netty_unaryMessageSizeLimit() throws Exception {
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
        // 200k triples with distinct literals is well over the 4 MiB default even with term tables.
        java.util.function.Consumer<org.apache.jena.riot.system.StreamRDF> big = s -> {
            for ( int i = 0; i < 200_000; i++ )
                s.triple(Triple.create(NodeFactory.createURI("http://example/s" + i),
                                       NodeFactory.createURI("http://example/p"),
                                       NodeFactory.createLiteralString("literal value number " + i)));
        };
        try ( RDFGrpcServer limited = RDFGrpcServer.newBuilder().port(0).dataset(dsg).build().start() ) {
            ManagedChannel ch = NettyChannelBuilder.forAddress("127.0.0.1", limited.getPort()).usePlaintext().build();
            try {
                assertThrows(ARQException.class, () -> RDFGrpcUnary.putRdf(ch, datasetHeader(), big, true, true));
                Txn.executeRead(dsg, () -> assertTrue(dsg.isEmpty()));
                // The streaming calls carry the same data as a sequence of small frames.
                try ( RDFGrpcSession session = RDFGrpcSession.open(ch) ) {
                    assertEquals(200_000, session.write(datasetHeader(), big).get(30, TimeUnit.SECONDS).getQuadCount());
                }
            } finally {
                ch.shutdownNow();
            }
        }
        try ( RDFGrpcServer raised = RDFGrpcServer.newBuilder().port(0).dataset(dsg)
                                                  .maxInboundMessageSize(256 * 1024 * 1024).build().start() ) {
            ManagedChannel ch = NettyChannelBuilder.forAddress("127.0.0.1", raised.getPort())
                .maxInboundMessageSize(256 * 1024 * 1024).usePlaintext().build();
            try {
                WriteRdfHeader replace = WriteRdfHeader.newBuilder()
                    .setEntireDataset(true).setDisposition(WriteDisposition.WRITE_DISPOSITION_REPLACE).build();
                assertEquals(200_000, RDFGrpcUnary.putRdf(ch, replace, big, true, true).getQuadCount());
                org.apache.jena.riot.lang.StreamRDFCounting counter = StreamRDFLib.count();
                RDFGrpcUnary.getRdf(ch, entireDataset(), counter);
                assertEquals(200_000, counter.count());
            } finally {
                ch.shutdownNow();
            }
        }
    }
}
