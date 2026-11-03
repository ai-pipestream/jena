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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCalls;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.grpc.RDFGrpcFrames.StreamRdfFrame;
import org.apache.jena.grpc.wire.PB_RDFService.StreamRdfRequest;
import org.apache.jena.grpc.wire.PB_RDFService.WriteRdfHeader;
import org.apache.jena.grpc.wire.PB_RDFService.WriteRdfResponse;
import org.apache.jena.riot.protobuf.ProtobufRDF;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFBase;
import org.apache.jena.riot.system.StreamRDFOps;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.DatasetGraphSink;
import org.apache.jena.sys.JenaSystem;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;

/**
 * Times the RDF transfer calls of {@link RDFDatasetGrpcService} against each other. Not a
 * test; run its {@code main}.
 * <pre>
 * BenchGrpcTransport [inprocess|netty] [triples] [documents] [triplesPerDocument] [rounds]
 * </pre>
 * Bulk reads stream {@code triples} triples from a list-backed graph into a counting sink
 * through the server-streaming call, the unary call and the bidirectional session, with
 * and without term tables (RDF Protobuf version 2 and 1). Bulk writes send the same triples
 * into a discarding dataset through the client-streaming call, the unary call and the
 * session. The document runs send {@code documents} graphs of {@code triplesPerDocument}
 * triples each, one write per document, and compare a call per document with pipelined
 * writes on one session. The first three rounds of each run warm up and are not reported.
 */
public class BenchGrpcTransport {

    static final int WARMUP = 3;

    public static void main(String... args) throws Exception {
        JenaSystem.init();
        boolean netty = args.length > 0 && args[0].equalsIgnoreCase("netty");
        int n = args.length > 1 ? Integer.parseInt(args[1]) : 1_000_000;
        int docs = args.length > 2 ? Integer.parseInt(args[2]) : 10_000;
        int docSize = args.length > 3 ? Integer.parseInt(args[3]) : 100;
        int rounds = args.length > 4 ? Integer.parseInt(args[4]) : 5;

        List<Triple> triples = generate(n);
        Graph source = new ListGraph(triples);
        DatasetGraph readDsg = DatasetGraphFactory.wrap(source);
        DatasetGraph sink = DatasetGraphSink.create();
        List<Graph> documents = new ArrayList<>(docs);
        for ( int d = 0; d < docs; d++ )
            documents.add(new ListGraph(generate(docSize, d * 7919L)));

        System.out.printf("Transport: %s  Triples: %,d  Documents: %,d x %,d  Rounds: %d (after %d warm-up)%n",
                          netty ? "netty (127.0.0.1)" : "in-process", n, docs, docSize, rounds, WARMUP);

        try ( Target read2 = Target.start(readDsg, true, netty);
              Target read1 = Target.start(readDsg, false, netty);
              Target write = Target.start(sink, true, netty) ) {

            System.out.println();
            System.out.println("Bulk read, " + n + " triples into a counting sink");
            time("StreamRdf   v2", rounds, n, () -> streamRdf(read2, n));
            time("GetRdf      v2", rounds, n, () -> getRdf(read2, n));
            time("RdfSession  v2", rounds, n, () -> sessionRead(read2, n));
            time("StreamRdf   v1", rounds, n, () -> streamRdf(read1, n));
            time("GetRdf      v1", rounds, n, () -> getRdf(read1, n));
            time("RdfSession  v1", rounds, n, () -> sessionRead(read1, n));

            System.out.println();
            System.out.println("Bulk write, " + n + " triples into a discarding dataset");
            time("WriteRdf    v2", rounds, n, () -> writeRdf(write, source, true));
            time("PutRdf      v2", rounds, n, () -> putRdf(write, source, true));
            time("RdfSession  v2", rounds, n, () -> sessionWrite(write, source, true));
            time("WriteRdf    v1", rounds, n, () -> writeRdf(write, source, false));
            time("PutRdf      v1", rounds, n, () -> putRdf(write, source, false));
            time("RdfSession  v1", rounds, n, () -> sessionWrite(write, source, false));

            System.out.println();
            System.out.println("Documents, " + docs + " writes of " + docSize + " triples (v2)");
            int total = docs * docSize;
            time("PutRdf per document        ", rounds, total, docs, () -> docsUnary(write, documents));
            time("WriteRdf per document      ", rounds, total, docs, () -> docsWriteRdf(write, documents));
            time("RdfSession, wait each      ", rounds, total, docs, () -> docsSession(write, documents, false));
            time("RdfSession, pipelined      ", rounds, total, docs, () -> docsSession(write, documents, true));
        }
    }

    // ---- Scenarios

    static void streamRdf(Target t, int expected) {
        Counter c = new Counter();
        Iterator<StreamRdfFrame> iter = ClientCalls.blockingServerStreamingCall(
            t.channel, RDFGrpcProtocol.STREAM_RDF_METHOD, CallOptions.DEFAULT, defaultGraph());
        Consumer<ByteBuffer> sink = ProtobufRDF.framesToStreamRDF(c);
        c.start();
        while ( iter.hasNext() )
            sink.accept(iter.next().rows());
        c.finish();
        check(c.count, expected);
    }

    static void getRdf(Target t, int expected) {
        Counter c = new Counter();
        RDFGrpcUnary.getRdf(t.channel, defaultGraph(), c);
        check(c.count, expected);
    }

    static void sessionRead(Target t, int expected) throws Exception {
        Counter c = new Counter();
        try ( RDFGrpcSession session = RDFGrpcSession.open(t.channel) ) {
            session.read(defaultGraph(), c).get();
        }
        check(c.count, expected);
    }

    static void writeRdf(Target t, Graph source, boolean termTables) {
        try ( RDFLinkGrpc link = RDFLinkGrpc.newBuilder().channel(t.channel).termTables(termTables).build() ) {
            link.load(source);
        }
    }

    static void putRdf(Target t, Graph source, boolean termTables) {
        RDFGrpcUnary.putRdf(t.channel, dataset(), s -> StreamRDFOps.sendGraphToStream(source, s), true, termTables);
    }

    static void sessionWrite(Target t, Graph source, boolean termTables) throws Exception {
        try ( RDFGrpcSession session = RDFGrpcSession.open(t.channel, CallOptions.DEFAULT, true, termTables, ProtobufRDF.DEFAULT_BATCH_SIZE) ) {
            session.write(dataset(), s -> StreamRDFOps.sendGraphToStream(source, s)).get();
        }
    }

    static void docsUnary(Target t, List<Graph> documents) {
        for ( Graph doc : documents )
            RDFGrpcUnary.putRdf(t.channel, dataset(), s -> StreamRDFOps.sendGraphToStream(doc, s), true, true);
    }

    static void docsWriteRdf(Target t, List<Graph> documents) {
        try ( RDFLinkGrpc link = RDFLinkGrpc.newBuilder().channel(t.channel).build() ) {
            for ( Graph doc : documents )
                link.load(doc);
        }
    }

    static void docsSession(Target t, List<Graph> documents, boolean pipelined) throws Exception {
        try ( RDFGrpcSession session = RDFGrpcSession.open(t.channel) ) {
            List<CompletableFuture<WriteRdfResponse>> pending = new ArrayList<>();
            for ( Graph doc : documents ) {
                CompletableFuture<WriteRdfResponse> r = session.write(dataset(), s -> StreamRDFOps.sendGraphToStream(doc, s));
                if ( pipelined )
                    pending.add(r);
                else
                    r.get();
            }
            for ( CompletableFuture<WriteRdfResponse> r : pending )
                r.get();
        }
    }

    // ---- Plumbing

    static StreamRdfRequest defaultGraph() {
        return StreamRdfRequest.newBuilder().setGraph(RDFGrpcProtocol.encodeGraphTarget(null)).build();
    }

    static WriteRdfHeader dataset() {
        return WriteRdfHeader.newBuilder().setEntireDataset(true).build();
    }

    /** A server on a dataset plus a channel to it. */
    static final class Target implements AutoCloseable {
        final RDFGrpcServer server;
        final ManagedChannel channel;

        static Target start(DatasetGraph dsg, boolean termTables, boolean netty) {
            RDFGrpcServer.Builder builder = RDFGrpcServer.newBuilder().dataset(dsg).termTables(termTables)
                .maxInboundMessageSize(Integer.MAX_VALUE);
            RDFGrpcServer server;
            ManagedChannel channel;
            if ( netty ) {
                server = builder.host("127.0.0.1").port(0).build().start();
                channel = NettyChannelBuilder.forAddress("127.0.0.1", server.getPort())
                    .maxInboundMessageSize(Integer.MAX_VALUE).usePlaintext().build();
            } else {
                String name = "bench-" + System.nanoTime();
                server = builder.inProcessName(name).build().start();
                channel = InProcessChannelBuilder.forName(name).directExecutor().build();
            }
            return new Target(server, channel);
        }

        private Target(RDFGrpcServer server, ManagedChannel channel) {
            this.server = server;
            this.channel = channel;
        }

        @Override
        public void close() {
            channel.shutdownNow();
            server.close();
        }
    }

    interface Run { void run() throws Exception; }

    static void time(String label, int rounds, int triples, Run run) throws Exception {
        time(label, rounds, triples, 0, run);
    }

    static void time(String label, int rounds, int triples, int docs, Run run) throws Exception {
        double[] ms = new double[rounds];
        for ( int i = 0; i < WARMUP + rounds; i++ ) {
            long t0 = System.nanoTime();
            run.run();
            long t1 = System.nanoTime();
            if ( i >= WARMUP )
                ms[i - WARMUP] = (t1 - t0) / 1e6;
        }
        double[] s = ms.clone();
        Arrays.sort(s);
        double median = s[s.length / 2];
        if ( docs > 0 )
            System.out.printf("%s  median %,9.1f ms  (min %,9.1f ms)  %,10.0f triples/s  %,8.0f docs/s%n",
                              label, median, s[0], triples / (median / 1000.0), docs / (median / 1000.0));
        else
            System.out.printf("%s  median %,9.1f ms  (min %,9.1f ms)  %,10.0f triples/s%n",
                              label, median, s[0], triples / (median / 1000.0));
    }

    static void check(long count, int expected) {
        if ( count != expected )
            throw new IllegalStateException("Read " + count + " triples, expected " + expected);
    }

    static final class Counter extends StreamRDFBase {
        long count = 0;
        @Override public void triple(Triple t) { count++; }
    }

    /** A read-only graph over a list, so that reads are not bound by a store's find. */
    static final class ListGraph extends GraphBase {
        private final List<Triple> triples;

        ListGraph(List<Triple> triples) {
            this.triples = triples;
        }

        @Override
        protected ExtendedIterator<Triple> graphBaseFind(Triple m) {
            ExtendedIterator<Triple> iter = WrappedIterator.create(triples.iterator());
            if ( Triple.ANY.equals(m) )
                return iter;
            return iter.filterKeep(m::matches);
        }

        @Override
        protected int graphBaseSize() {
            return triples.size();
        }
    }

    static List<Triple> generate(int n) {
        return generate(n, 42);
    }

    /** The same mix of term kinds as the RDF Protobuf format benchmark. */
    static List<Triple> generate(int n, long seed) {
        List<Triple> triples = new ArrayList<>(n);
        Random rnd = new Random(seed);
        Node[] preds = new Node[20];
        for ( int i = 0; i < preds.length; i++ )
            preds[i] = NodeFactory.createURI("http://example.org/vocab#p" + i);
        Node rdfType = NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        for ( int i = 0; i < n; i++ ) {
            Node s = NodeFactory.createURI("http://example.org/data/" + seed + "/item" + (i / 10));
            Node o;
            switch (i % 10) {
                case 0 -> o = NodeFactory.createURI("http://example.org/data/item" + rnd.nextInt(Math.max(1, i / 10 + 1)));
                case 1 -> o = NodeFactory.createLiteralString("Some text value number " + i);
                case 2 -> o = NodeFactory.createLiteralDT(Integer.toString(rnd.nextInt(100000)), XSDDatatype.XSDinteger);
                case 3 -> o = NodeFactory.createLiteralLang("Label " + i, "en");
                case 4 -> o = NodeFactory.createLiteralDT(Double.toString(rnd.nextDouble()), XSDDatatype.XSDdouble);
                case 5 -> o = NodeFactory.createBlankNode("b" + (i / 10));
                case 6 -> o = NodeFactory.createLiteralDT("2026-11-03T12:00:" + String.format("%02d", i % 60), XSDDatatype.XSDdateTime);
                default -> o = NodeFactory.createURI("http://example.org/class/C" + (i % 37));
            }
            Node p = (i % 10 == 9) ? rdfType : preds[i % preds.length];
            triples.add(Triple.create(s, p, o));
        }
        return triples;
    }
}
