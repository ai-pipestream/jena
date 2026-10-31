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

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.google.protobuf.BoolValue;
import com.google.protobuf.Empty;
import com.google.protobuf.StringValue;

import io.grpc.BindableService;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonArray;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.Syntax;
import org.apache.jena.query.TxnType;
import org.apache.jena.rdfpatch.RDFPatch;
import org.apache.jena.rdfpatch.RDFPatchOps;
import org.apache.jena.rdfpatch.changes.RDFChangesApply;
import org.apache.jena.riot.protobuf.ProtobufConvert;
import org.apache.jena.riot.protobuf.ProtobufRDF;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Patch;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Patch_Row;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Quad;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Stream;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_StreamRow;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Term;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.riot.system.StreamRDFOps;
import org.apache.jena.sparql.JenaTransactionException;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Transactional.Promote;
import org.apache.jena.sparql.exec.QueryExec;
import org.apache.jena.sparql.exec.RowSet;
import org.apache.jena.sparql.exec.UpdateExec;
import org.apache.jena.system.Txn;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;

/**
 * gRPC {@link BindableService} exposing a Jena {@link DatasetGraph} over {@code PB_RDF}
 * streaming messages, SPARQL Query/Update, and {@link RDFPatch} replication without
 * Jetty or Servlet dependencies.
 */
public class RDFDatasetGrpcService implements BindableService, AutoCloseable {

    private static final String ERR_DSG_NULL = "dsg must not be null";
    private static final String ERR_PATCH_NULL = "patch must not be null";
    private static final int DEFAULT_BATCH_SIZE = ProtobufRDF.DEFAULT_BATCH_SIZE;

    /** Default time a remote transaction may stay idle before the server aborts and ends it. */
    public static final Duration DEFAULT_TXN_IDLE_TIMEOUT = Duration.ofMinutes(5);

    private final DatasetGraph dsg;
    private final boolean encodeValues;
    private final int batchSize;
    private final long txnIdleTimeoutNanos;
    private final Map<String, ServerTxnSession> activeTxns = new ConcurrentHashMap<>();
    private final PatchPublisher patchPublisher = new PatchPublisher();
    private final ScheduledExecutorService txnReaper;

    /**
     * Create a gRPC service backed by {@code dsg} with value encoding enabled and default batch size.
     *
     * @param dsg the backing {@link DatasetGraph}
     * @throws IllegalArgumentException Thrown if {@code dsg} is {@code null}
     */
    public RDFDatasetGrpcService(DatasetGraph dsg) {
        this(dsg, true, DEFAULT_BATCH_SIZE);
    }

    /**
     * Create a gRPC service backed by {@code dsg} with configurable value encoding and batch size.
     *
     * @param dsg the backing {@link DatasetGraph}
     * @param encodeValues {@code true} to encode numeric literals by value in {@code PB_RDF} terms
     * @param batchSize number of rows per {@link RDF_Stream} batch
     * @throws IllegalArgumentException Thrown if {@code dsg} is {@code null} or {@code batchSize <= 0}
     */
    public RDFDatasetGrpcService(DatasetGraph dsg, boolean encodeValues, int batchSize) {
        this(dsg, encodeValues, batchSize, DEFAULT_TXN_IDLE_TIMEOUT);
    }

    /**
     * Create a gRPC service backed by {@code dsg} with configurable value encoding, batch size,
     * and remote transaction idle timeout.
     * <p>
     * A remote transaction that has had no request for longer than {@code txnIdleTimeout} is aborted
     * and ended by the server, so a client that disappears cannot hold the dataset's writer lock forever.
     *
     * @param dsg the backing {@link DatasetGraph}
     * @param encodeValues {@code true} to encode numeric literals by value in {@code PB_RDF} terms
     * @param batchSize number of rows per {@link RDF_Stream} batch
     * @param txnIdleTimeout maximum idle time of a remote transaction
     * @throws IllegalArgumentException Thrown if {@code dsg} or {@code txnIdleTimeout} is {@code null},
     *     {@code batchSize <= 0}, or {@code txnIdleTimeout} is not positive
     */
    public RDFDatasetGrpcService(DatasetGraph dsg, boolean encodeValues, int batchSize, Duration txnIdleTimeout) {
        if ( dsg == null )
            throw new IllegalArgumentException(ERR_DSG_NULL);
        if ( batchSize <= 0 )
            throw new IllegalArgumentException("batchSize must be positive: " + batchSize);
        if ( txnIdleTimeout == null || txnIdleTimeout.isNegative() || txnIdleTimeout.isZero() )
            throw new IllegalArgumentException("txnIdleTimeout must be positive: " + txnIdleTimeout);
        this.dsg = dsg;
        this.encodeValues = encodeValues;
        this.batchSize = batchSize;
        this.txnIdleTimeoutNanos = txnIdleTimeout.toNanos();
        this.txnReaper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "jena-grpc-txn-reaper");
            t.setDaemon(true);
            return t;
        });
        long periodMillis = Math.clamp(txnIdleTimeout.toMillis() / 4, 10L, 30_000L);
        txnReaper.scheduleWithFixedDelay(this::reapIdleTxns, periodMillis, periodMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Return the backing {@link DatasetGraph}.
     *
     * @return the dataset graph
     */
    public DatasetGraph getDatasetGraph() {
        return dsg;
    }

    /** {@inheritDoc} */
    @Override
    public ServerServiceDefinition bindService() {
        ServerServiceDefinition raw = ServerServiceDefinition.builder(RDFGrpcProtocol.SERVICE_NAME)
            .addMethod(RDFGrpcProtocol.FIND_STREAM_METHOD,
                ServerCalls.asyncServerStreamingCall(this::findStream))
            .addMethod(RDFGrpcProtocol.FIND_STREAM_BATCHED_METHOD,
                ServerCalls.asyncServerStreamingCall(this::findStreamBatched))
            .addMethod(RDFGrpcProtocol.GET_GRAPH_METHOD,
                ServerCalls.asyncServerStreamingCall(this::getGraph))
            .addMethod(RDFGrpcProtocol.LOAD_GRAPH_METHOD,
                ServerCalls.asyncClientStreamingCall(this::loadGraph))
            .addMethod(RDFGrpcProtocol.PUT_GRAPH_METHOD,
                ServerCalls.asyncClientStreamingCall(this::putGraph))
            .addMethod(RDFGrpcProtocol.DELETE_GRAPH_METHOD,
                ServerCalls.asyncUnaryCall(this::deleteGraph))
            .addMethod(RDFGrpcProtocol.GET_DATASET_METHOD,
                ServerCalls.asyncServerStreamingCall(this::getDataset))
            .addMethod(RDFGrpcProtocol.LOAD_DATASET_METHOD,
                ServerCalls.asyncClientStreamingCall(this::loadDataset))
            .addMethod(RDFGrpcProtocol.LOAD_DATASET_BATCHED_METHOD,
                ServerCalls.asyncClientStreamingCall(this::loadDatasetBatched))
            .addMethod(RDFGrpcProtocol.PUT_DATASET_METHOD,
                ServerCalls.asyncClientStreamingCall(this::putDataset))
            .addMethod(RDFGrpcProtocol.CLEAR_DATASET_METHOD,
                ServerCalls.asyncUnaryCall(this::clearDataset))
            .addMethod(RDFGrpcProtocol.QUERY_SELECT_METHOD,
                ServerCalls.asyncServerStreamingCall(this::querySelect))
            .addMethod(RDFGrpcProtocol.QUERY_GRAPH_METHOD,
                ServerCalls.asyncServerStreamingCall(this::queryGraph))
            .addMethod(RDFGrpcProtocol.QUERY_DATASET_METHOD,
                ServerCalls.asyncServerStreamingCall(this::queryDataset))
            .addMethod(RDFGrpcProtocol.QUERY_ASK_METHOD,
                ServerCalls.asyncUnaryCall(this::queryAsk))
            .addMethod(RDFGrpcProtocol.QUERY_JSON_METHOD,
                ServerCalls.asyncUnaryCall(this::queryJson))
            .addMethod(RDFGrpcProtocol.EXECUTE_UPDATE_METHOD,
                ServerCalls.asyncUnaryCall(this::executeUpdate))
            .addMethod(RDFGrpcProtocol.APPLY_PATCH_METHOD,
                ServerCalls.asyncClientStreamingCall(this::applyPatch))
            .addMethod(RDFGrpcProtocol.APPLY_PATCH_BATCH_METHOD,
                ServerCalls.asyncUnaryCall(this::applyPatchBatch))
            .addMethod(RDFGrpcProtocol.SUBSCRIBE_PATCHES_METHOD,
                ServerCalls.asyncServerStreamingCall(this::subscribePatches))
            .addMethod(RDFGrpcProtocol.BEGIN_TXN_METHOD,
                ServerCalls.asyncUnaryCall(this::beginTxn))
            .addMethod(RDFGrpcProtocol.COMMIT_TXN_METHOD,
                ServerCalls.asyncUnaryCall(this::commitTxn))
            .addMethod(RDFGrpcProtocol.ABORT_TXN_METHOD,
                ServerCalls.asyncUnaryCall(this::abortTxn))
            .addMethod(RDFGrpcProtocol.END_TXN_METHOD,
                ServerCalls.asyncUnaryCall(this::endTxn))
            .addMethod(RDFGrpcProtocol.PROMOTE_TXN_METHOD,
                ServerCalls.asyncUnaryCall(this::promoteTxn))
            .build();
        return ServerInterceptors.intercept(raw, metadataInterceptor());
    }

    /**
     * Broadcast an {@link RDFPatch} as {@link RDF_Patch_Row} messages to all active patch subscribers.
     *
     * @param patch the patch to broadcast
     * @throws IllegalArgumentException Thrown if {@code patch} is {@code null}
     */
    public void publishPatch(RDFPatch patch) {
        if ( patch == null )
            throw new IllegalArgumentException(ERR_PATCH_NULL);
        if ( !patchPublisher.hasSubscribers() )
            return;
        List<RDF_Patch_Row> rows = new ArrayList<>();
        RDFPatchOps.writeProtobuf(patch, rows::add);
        patchPublisher.reserve().publish(rows);
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        txnReaper.shutdownNow();
        for ( String id : new ArrayList<>(activeTxns.keySet()) ) {
            ServerTxnSession session = activeTxns.remove(id);
            if ( session != null ) {
                session.close();
            }
        }
        patchPublisher.close();
    }

    /** Number of open remote transactions (for tests). */
    int activeTxnCount() {
        return activeTxns.size();
    }

    private void reapIdleTxns() {
        for ( Map.Entry<String, ServerTxnSession> e : activeTxns.entrySet() ) {
            ServerTxnSession session = e.getValue();
            if ( session.isIdleFor(txnIdleTimeoutNanos) && activeTxns.remove(e.getKey(), session) ) {
                Log.warn(this, "Remote transaction idle timeout, aborting: " + e.getKey());
                session.close();
            }
        }
    }

    private ServerInterceptor metadataInterceptor() {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call,
                    Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                Context ctx = Context.current();
                String txnId = headers.get(RDFGrpcProtocol.META_TXN_ID);
                if ( txnId != null ) {
                    ctx = ctx.withValue(RDFGrpcProtocol.CTX_TXN_ID, txnId);
                }
                byte[] graphBytes = headers.get(RDFGrpcProtocol.META_GRAPH_TERM_BIN);
                if ( graphBytes != null ) {
                    ctx = ctx.withValue(RDFGrpcProtocol.CTX_GRAPH_TERM_BIN, graphBytes);
                }
                return Contexts.interceptCall(ctx, call, headers, next);
            }
        };
    }

    private void findStream(RDF_Quad request, StreamObserver<RDF_StreamRow> responseObserver) {
        String txnId = RDFGrpcProtocol.CTX_TXN_ID.get();
        try {
            Quad pattern = ProtobufConvert.convert(request);
            Node g = normalizePatternNode(pattern.getGraph());
            Node s = normalizePatternNode(pattern.getSubject());
            Node p = normalizePatternNode(pattern.getPredicate());
            Node o = normalizePatternNode(pattern.getObject());
            executeRead(txnId, () -> {
                StreamRDF encoder = ProtobufRDF.streamToRowConsumer(responseObserver::onNext, encodeValues);
                encoder.start();
                Iterator<Quad> iter = dsg.find(g, s, p, o);
                while ( iter.hasNext() ) {
                    encoder.quad(iter.next());
                }
                encoder.finish();
            });
            responseObserver.onCompleted();
        } catch (Throwable ex) {
            responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
        }
    }

    private void findStreamBatched(RDF_Quad request, StreamObserver<RDF_Stream> responseObserver) {
        String txnId = RDFGrpcProtocol.CTX_TXN_ID.get();
        try {
            Quad pattern = ProtobufConvert.convert(request);
            Node g = normalizePatternNode(pattern.getGraph());
            Node s = normalizePatternNode(pattern.getSubject());
            Node p = normalizePatternNode(pattern.getPredicate());
            Node o = normalizePatternNode(pattern.getObject());
            executeRead(txnId, () -> {
                StreamRDF encoder = ProtobufRDF.streamToBatchConsumer(responseObserver::onNext, batchSize, encodeValues);
                encoder.start();
                Iterator<Quad> iter = dsg.find(g, s, p, o);
                while ( iter.hasNext() ) {
                    encoder.quad(iter.next());
                }
                encoder.finish();
            });
            responseObserver.onCompleted();
        } catch (Throwable ex) {
            responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
        }
    }

    private void getGraph(RDF_Term request, StreamObserver<RDF_StreamRow> responseObserver) {
        String txnId = RDFGrpcProtocol.CTX_TXN_ID.get();
        try {
            Node graphName = RDFGrpcProtocol.decodeGraphNode(request);
            executeRead(txnId, () -> {
                Graph graph = (graphName == null) ? dsg.getDefaultGraph() : dsg.getGraph(graphName);
                StreamRDF encoder = ProtobufRDF.streamToRowConsumer(responseObserver::onNext, encodeValues);
                encoder.start();
                if ( graph != null ) {
                    StreamRDFOps.sendGraphToStream(graph, encoder);
                }
                encoder.finish();
            });
            responseObserver.onCompleted();
        } catch (Throwable ex) {
            responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
        }
    }

    private StreamObserver<RDF_StreamRow> loadGraph(StreamObserver<Empty> responseObserver) {
        String txnId = RDFGrpcProtocol.CTX_TXN_ID.get();
        Node graphName = RDFGrpcProtocol.decodeGraphNodeBytes(RDFGrpcProtocol.CTX_GRAPH_TERM_BIN.get());
        return collectRowsObserver(responseObserver, rows -> executeWrite(txnId, () -> {
            Graph target = (graphName == null) ? dsg.getDefaultGraph() : dsg.getGraph(graphName);
            StreamRDF dest = StreamRDFLib.graph(target);
            Consumer<RDF_StreamRow> sink = ProtobufRDF.rowConsumerToStreamRDF(dest);
            dest.start();
            rows.forEach(sink);
            dest.finish();
        }));
    }

    private StreamObserver<RDF_StreamRow> putGraph(StreamObserver<Empty> responseObserver) {
        String txnId = RDFGrpcProtocol.CTX_TXN_ID.get();
        Node graphName = RDFGrpcProtocol.decodeGraphNodeBytes(RDFGrpcProtocol.CTX_GRAPH_TERM_BIN.get());
        return collectRowsObserver(responseObserver, rows -> executeWrite(txnId, () -> {
            Graph target = (graphName == null) ? dsg.getDefaultGraph() : dsg.getGraph(graphName);
            target.clear();
            StreamRDF dest = StreamRDFLib.graph(target);
            Consumer<RDF_StreamRow> sink = ProtobufRDF.rowConsumerToStreamRDF(dest);
            dest.start();
            rows.forEach(sink);
            dest.finish();
        }));
    }

    private void deleteGraph(RDF_Term request, StreamObserver<Empty> responseObserver) {
        String txnId = RDFGrpcProtocol.CTX_TXN_ID.get();
        try {
            Node graphName = RDFGrpcProtocol.decodeGraphNode(request);
            executeWrite(txnId, () -> {
                if ( graphName == null ) {
                    dsg.getDefaultGraph().clear();
                } else {
                    dsg.removeGraph(graphName);
                }
            });
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Throwable ex) {
            responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
        }
    }

    private void getDataset(Empty request, StreamObserver<RDF_StreamRow> responseObserver) {
        String txnId = RDFGrpcProtocol.CTX_TXN_ID.get();
        try {
            executeRead(txnId, () -> {
                StreamRDF encoder = ProtobufRDF.streamToRowConsumer(responseObserver::onNext, encodeValues);
                encoder.start();
                StreamRDFOps.sendDatasetToStream(dsg, encoder);
                encoder.finish();
            });
            responseObserver.onCompleted();
        } catch (Throwable ex) {
            responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
        }
    }

    private StreamObserver<RDF_StreamRow> loadDataset(StreamObserver<Empty> responseObserver) {
        String txnId = RDFGrpcProtocol.CTX_TXN_ID.get();
        return collectRowsObserver(responseObserver, rows -> executeWrite(txnId, () -> {
            StreamRDF dest = StreamRDFLib.dataset(dsg);
            Consumer<RDF_StreamRow> sink = ProtobufRDF.rowConsumerToStreamRDF(dest);
            dest.start();
            rows.forEach(sink);
            dest.finish();
        }));
    }

    private StreamObserver<RDF_Stream> loadDatasetBatched(StreamObserver<Empty> responseObserver) {
        String txnId = RDFGrpcProtocol.CTX_TXN_ID.get();
        List<RDF_Stream> batches = new ArrayList<>();
        return new StreamObserver<>() {
            @Override
            public void onNext(RDF_Stream value) {
                batches.add(value);
            }

            @Override
            public void onError(Throwable t) {
                batches.clear();
            }

            @Override
            public void onCompleted() {
                try {
                    executeWrite(txnId, () -> {
                        StreamRDF dest = StreamRDFLib.dataset(dsg);
                        Consumer<RDF_Stream> sink = ProtobufRDF.batchConsumerToStreamRDF(dest);
                        dest.start();
                        batches.forEach(sink);
                        dest.finish();
                    });
                    responseObserver.onNext(Empty.getDefaultInstance());
                    responseObserver.onCompleted();
                } catch (Throwable ex) {
                    responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
                }
            }
        };
    }

    private StreamObserver<RDF_StreamRow> putDataset(StreamObserver<Empty> responseObserver) {
        String txnId = RDFGrpcProtocol.CTX_TXN_ID.get();
        return collectRowsObserver(responseObserver, rows -> executeWrite(txnId, () -> {
            dsg.clear();
            StreamRDF dest = StreamRDFLib.dataset(dsg);
            Consumer<RDF_StreamRow> sink = ProtobufRDF.rowConsumerToStreamRDF(dest);
            dest.start();
            rows.forEach(sink);
            dest.finish();
        }));
    }

    private void clearDataset(Empty request, StreamObserver<Empty> responseObserver) {
        String txnId = RDFGrpcProtocol.CTX_TXN_ID.get();
        try {
            executeWrite(txnId, dsg::clear);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Throwable ex) {
            responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
        }
    }

    private void querySelect(StringValue request, StreamObserver<RDFSelectMessage> responseObserver) {
        String txnId = RDFGrpcProtocol.CTX_TXN_ID.get();
        try {
            Query query = QueryFactory.create(request.getValue(), Syntax.syntaxARQ);
            executeRead(txnId, () -> {
                try ( QueryExec qExec = QueryExec.dataset(dsg).query(query).build() ) {
                    RowSet rs = qExec.select();
                    ProtobufRDF.writeRowSet(
                        rs,
                        hdr -> responseObserver.onNext(RDFSelectMessage.ofHeader(hdr)),
                        row -> responseObserver.onNext(RDFSelectMessage.ofRow(row)),
                        encodeValues);
                }
            });
            responseObserver.onCompleted();
        } catch (Throwable ex) {
            responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
        }
    }

    private void queryGraph(StringValue request, StreamObserver<RDF_StreamRow> responseObserver) {
        String txnId = RDFGrpcProtocol.CTX_TXN_ID.get();
        try {
            Query query = QueryFactory.create(request.getValue(), Syntax.syntaxARQ);
            executeRead(txnId, () -> {
                try ( QueryExec qExec = QueryExec.dataset(dsg).query(query).build() ) {
                    StreamRDF encoder = ProtobufRDF.streamToRowConsumer(responseObserver::onNext, encodeValues);
                    encoder.start();
                    if ( query.isDescribeType() ) {
                        Graph g = qExec.describe();
                        StreamRDFOps.sendGraphToStream(g, encoder);
                    } else {
                        Graph g = qExec.construct();
                        StreamRDFOps.sendGraphToStream(g, encoder);
                    }
                    encoder.finish();
                }
            });
            responseObserver.onCompleted();
        } catch (Throwable ex) {
            responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
        }
    }

    private void queryDataset(StringValue request, StreamObserver<RDF_StreamRow> responseObserver) {
        String txnId = RDFGrpcProtocol.CTX_TXN_ID.get();
        try {
            Query query = QueryFactory.create(request.getValue(), Syntax.syntaxARQ);
            executeRead(txnId, () -> {
                try ( QueryExec qExec = QueryExec.dataset(dsg).query(query).build() ) {
                    DatasetGraph resultDsg = qExec.constructDataset();
                    StreamRDF encoder = ProtobufRDF.streamToRowConsumer(responseObserver::onNext, encodeValues);
                    encoder.start();
                    StreamRDFOps.sendDatasetToStream(resultDsg, encoder);
                    encoder.finish();
                }
            });
            responseObserver.onCompleted();
        } catch (Throwable ex) {
            responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
        }
    }

    private void queryAsk(StringValue request, StreamObserver<BoolValue> responseObserver) {
        String txnId = RDFGrpcProtocol.CTX_TXN_ID.get();
        try {
            Query query = QueryFactory.create(request.getValue(), Syntax.syntaxARQ);
            boolean answer = calculateRead(txnId, () -> {
                try ( QueryExec qExec = QueryExec.dataset(dsg).query(query).build() ) {
                    return qExec.ask();
                }
            });
            responseObserver.onNext(BoolValue.of(answer));
            responseObserver.onCompleted();
        } catch (Throwable ex) {
            responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
        }
    }

    private void queryJson(StringValue request, StreamObserver<StringValue> responseObserver) {
        String txnId = RDFGrpcProtocol.CTX_TXN_ID.get();
        try {
            Query query = QueryFactory.create(request.getValue(), Syntax.syntaxARQ);
            String jsonText = calculateRead(txnId, () -> {
                try ( QueryExec qExec = QueryExec.dataset(dsg).query(query).build() ) {
                    JsonArray array = qExec.execJson();
                    return JSON.toString(array);
                }
            });
            responseObserver.onNext(StringValue.of(jsonText));
            responseObserver.onCompleted();
        } catch (Throwable ex) {
            responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
        }
    }

    private void executeUpdate(StringValue request, StreamObserver<Empty> responseObserver) {
        String txnId = RDFGrpcProtocol.CTX_TXN_ID.get();
        try {
            UpdateRequest updateRequest = UpdateFactory.create(request.getValue(), Syntax.syntaxARQ);
            executeWrite(txnId, () -> UpdateExec.dataset(dsg).update(updateRequest).execute());
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Throwable ex) {
            responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
        }
    }

    private StreamObserver<RDF_Patch_Row> applyPatch(StreamObserver<Empty> responseObserver) {
        String txnId = RDFGrpcProtocol.CTX_TXN_ID.get();
        List<RDF_Patch_Row> rows = new ArrayList<>();
        return new StreamObserver<>() {
            @Override
            public void onNext(RDF_Patch_Row value) {
                rows.add(value);
            }

            @Override
            public void onError(Throwable t) {
                rows.clear();
            }

            @Override
            public void onCompleted() {
                try {
                    applyPatchRows(txnId, rows);
                    responseObserver.onNext(Empty.getDefaultInstance());
                    responseObserver.onCompleted();
                } catch (Throwable ex) {
                    responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
                }
            }
        };
    }

    private void applyPatchBatch(RDF_Patch request, StreamObserver<Empty> responseObserver) {
        String txnId = RDFGrpcProtocol.CTX_TXN_ID.get();
        try {
            applyPatchRows(txnId, request.getRowList());
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Throwable ex) {
            responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
        }
    }

    /**
     * Apply patch rows. Without a remote transaction the patch is applied in its own write transaction
     * and published to subscribers after it commits, in commit order. Inside a remote transaction the rows
     * are held by the session and published only when that transaction commits.
     */
    private void applyPatchRows(String txnId, List<RDF_Patch_Row> rows) throws Exception {
        List<RDF_Patch_Row> rowsCopy = List.copyOf(rows);
        if ( txnId != null ) {
            ServerTxnSession session = requireSession(txnId);
            session.run(() -> {
                applyRows(rowsCopy);
                session.addPendingPatchRows(rowsCopy);
            });
            return;
        }
        // The ticket is taken while holding the write lock, so ticket order is commit order.
        PatchPublisher.Ticket[] ticket = new PatchPublisher.Ticket[1];
        try {
            Txn.executeWrite(dsg, () -> {
                applyRows(rowsCopy);
                ticket[0] = patchPublisher.reserve();
            });
        } catch (Throwable ex) {
            if ( ticket[0] != null )
                ticket[0].cancel();
            throw ex;
        }
        ticket[0].publish(rowsCopy);
    }

    private void applyRows(List<RDF_Patch_Row> rows) {
        RDFChangesApply changes = new NonTransactionalChangesApply(dsg);
        Consumer<RDF_Patch_Row> sink = RDFPatchOps.protobufChangesReceiver(changes);
        changes.start();
        for ( RDF_Patch_Row row : rows ) {
            sink.accept(row);
        }
        changes.finish();
    }

    private void subscribePatches(Empty request, StreamObserver<RDF_Patch_Row> responseObserver) {
        if ( responseObserver instanceof ServerCallStreamObserver<RDF_Patch_Row> serverObs ) {
            serverObs.setOnCancelHandler(() -> patchPublisher.unsubscribe(responseObserver));
        }
        patchPublisher.subscribe(responseObserver);
    }

    private void beginTxn(StringValue request, StreamObserver<StringValue> responseObserver) {
        try {
            String modeStr = request.getValue();
            TxnType txnType = parseTxnType(modeStr);
            String txnId = UUID.randomUUID().toString();
            ServerTxnSession session = new ServerTxnSession(dsg, txnType);
            activeTxns.put(txnId, session);
            responseObserver.onNext(StringValue.of(txnId));
            responseObserver.onCompleted();
        } catch (Throwable ex) {
            responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
        }
    }

    private void commitTxn(StringValue request, StreamObserver<Empty> responseObserver) {
        try {
            ServerTxnSession session = requireSession(request.getValue());
            session.commit(patchPublisher);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Throwable ex) {
            responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
        }
    }

    private void abortTxn(StringValue request, StreamObserver<Empty> responseObserver) {
        try {
            ServerTxnSession session = requireSession(request.getValue());
            session.abort();
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Throwable ex) {
            responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
        }
    }

    private void endTxn(StringValue request, StreamObserver<Empty> responseObserver) {
        try {
            String txnId = request.getValue();
            ServerTxnSession session = activeTxns.remove(txnId);
            if ( session != null ) {
                session.end();
            }
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Throwable ex) {
            responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
        }
    }

    private void promoteTxn(StringValue request, StreamObserver<BoolValue> responseObserver) {
        try {
            String raw = request.getValue();
            int sep = raw.lastIndexOf(':');
            String txnId = (sep > 0) ? raw.substring(0, sep) : raw;
            String modeStr = (sep > 0) ? raw.substring(sep + 1) : Promote.ISOLATED.name();
            Promote promoteMode = "READ_COMMITTED".equalsIgnoreCase(modeStr)
                ? Promote.READ_COMMITTED
                : Promote.ISOLATED;
            ServerTxnSession session = requireSession(txnId);
            boolean promoted = session.call(() -> dsg.promote(promoteMode));
            responseObserver.onNext(BoolValue.of(promoted));
            responseObserver.onCompleted();
        } catch (Throwable ex) {
            responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
        }
    }

    private ServerTxnSession requireSession(String txnId) {
        ServerTxnSession session = activeTxns.get(txnId);
        if ( session == null )
            throw new JenaTransactionException("Unknown or expired remote transaction: " + txnId);
        return session;
    }

    private void executeRead(String txnId, Runnable action) throws Exception {
        if ( txnId != null ) {
            requireSession(txnId).run(action);
        } else {
            Txn.executeRead(dsg, action);
        }
    }

    private <T> T calculateRead(String txnId, Callable<T> action) throws Exception {
        if ( txnId != null ) {
            return requireSession(txnId).call(action);
        }
        return Txn.calculateRead(dsg, () -> {
            try {
                return action.call();
            } catch (RuntimeException rex) {
                throw rex;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });
    }

    private void executeWrite(String txnId, Runnable action) throws Exception {
        if ( txnId != null ) {
            requireSession(txnId).run(action);
        } else {
            Txn.executeWrite(dsg, action);
        }
    }

    private StreamObserver<RDF_StreamRow> collectRowsObserver(
            StreamObserver<Empty> responseObserver,
            ThrowingConsumer<List<RDF_StreamRow>> onCompleteAction) {
        List<RDF_StreamRow> rows = new ArrayList<>();
        return new StreamObserver<>() {
            @Override
            public void onNext(RDF_StreamRow value) {
                rows.add(value);
            }

            @Override
            public void onError(Throwable t) {
                rows.clear();
            }

            @Override
            public void onCompleted() {
                try {
                    onCompleteAction.accept(rows);
                    responseObserver.onNext(Empty.getDefaultInstance());
                    responseObserver.onCompleted();
                } catch (Throwable ex) {
                    responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
                }
            }
        };
    }

    private Node normalizePatternNode(Node node) {
        return (node == null) ? Node.ANY : node;
    }

    private TxnType parseTxnType(String modeStr) {
        if ( "READ".equalsIgnoreCase(modeStr) )
            return TxnType.READ;
        if ( "READ_PROMOTE".equalsIgnoreCase(modeStr) )
            return TxnType.READ_PROMOTE;
        if ( "READ_COMMITTED_PROMOTE".equalsIgnoreCase(modeStr) )
            return TxnType.READ_COMMITTED_PROMOTE;
        return TxnType.WRITE;
    }

    @FunctionalInterface
    private interface ThrowingConsumer<T> {
        void accept(T value) throws Exception;
    }

    /**
     * Applies patch data and prefix operations within an already active write transaction on {@link DatasetGraph}.
     */
    private static final class NonTransactionalChangesApply extends RDFChangesApply {

        NonTransactionalChangesApply(DatasetGraph dsg) {
            super(dsg);
        }

        @Override
        public void txnBegin() {
            // Enclosing executeWrite manages the transaction lifecycle
        }

        @Override
        public void txnCommit() {
            // Enclosing executeWrite manages the transaction lifecycle
        }

        @Override
        public void txnAbort() {
            throw new JenaTransactionException("Patch stream requested transaction abort (TA)");
        }
    }

    /**
     * Delivers patch rows to subscribers one patch at a time, in the order tickets were reserved.
     * All calls to a subscriber's {@link StreamObserver} happen while holding this object's lock,
     * because a gRPC {@link StreamObserver} must not be called concurrently.
     */
    private static final class PatchPublisher {
        // Guarded by "this"
        private final List<StreamObserver<RDF_Patch_Row>> subscribers = new ArrayList<>();
        private final ArrayDeque<Ticket> tickets = new ArrayDeque<>();

        /** A reserved place in the delivery order. Resolve it with {@link #publish} or {@link #cancel}. */
        final class Ticket {
            private List<RDF_Patch_Row> rows;
            private boolean resolved;

            void publish(List<RDF_Patch_Row> patchRows) {
                resolve(this, patchRows);
            }

            void cancel() {
                resolve(this, List.of());
            }
        }

        synchronized Ticket reserve() {
            Ticket ticket = new Ticket();
            tickets.addLast(ticket);
            return ticket;
        }

        synchronized boolean hasSubscribers() {
            return !subscribers.isEmpty();
        }

        synchronized void subscribe(StreamObserver<RDF_Patch_Row> subscriber) {
            subscribers.add(subscriber);
            // Ready marker: tells the client the subscription is registered.
            send(subscriber, RDF_Patch_Row.getDefaultInstance());
        }

        synchronized void unsubscribe(StreamObserver<RDF_Patch_Row> subscriber) {
            subscribers.remove(subscriber);
        }

        private synchronized void resolve(Ticket ticket, List<RDF_Patch_Row> patchRows) {
            if ( ticket.resolved )
                return;
            ticket.rows = patchRows;
            ticket.resolved = true;
            while ( !tickets.isEmpty() && tickets.peekFirst().resolved ) {
                deliver(tickets.removeFirst().rows);
            }
        }

        private void deliver(List<RDF_Patch_Row> patchRows) {
            if ( patchRows.isEmpty() )
                return;
            for ( StreamObserver<RDF_Patch_Row> sub : List.copyOf(subscribers) ) {
                for ( RDF_Patch_Row row : patchRows ) {
                    if ( !send(sub, row) )
                        break;
                }
            }
        }

        private boolean send(StreamObserver<RDF_Patch_Row> sub, RDF_Patch_Row row) {
            try {
                sub.onNext(row);
                return true;
            } catch (RuntimeException ex) {
                // Subscriber disconnected
                subscribers.remove(sub);
                return false;
            }
        }

        synchronized void close() {
            tickets.clear();
            for ( StreamObserver<RDF_Patch_Row> sub : subscribers ) {
                try {
                    sub.onCompleted();
                } catch (RuntimeException ignored) {
                    // Subscriber already disconnected
                }
            }
            subscribers.clear();
        }
    }

    /**
     * Pins a server-side {@link DatasetGraph} transaction to a dedicated single-thread executor
     * so multiple gRPC requests sharing a transaction ID execute on the same thread.
     * Patch rows applied in the transaction are held until commit.
     */
    private static final class ServerTxnSession {
        private final DatasetGraph dsg;
        private final ExecutorService executor;
        private final AtomicInteger inFlight = new AtomicInteger();
        // Only touched on the session thread
        private final List<RDF_Patch_Row> pendingPatchRows = new ArrayList<>();
        private volatile long lastUsed = System.nanoTime();
        private volatile boolean ended;

        ServerTxnSession(DatasetGraph dsg, TxnType txnType) throws Exception {
            this.dsg = dsg;
            this.executor = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "jena-grpc-txn");
                t.setDaemon(true);
                return t;
            });
            run(() -> dsg.begin(txnType));
        }

        void run(Runnable runnable) throws Exception {
            call(() -> {
                runnable.run();
                return null;
            });
        }

        <T> T call(Callable<T> callable) throws Exception {
            inFlight.incrementAndGet();
            lastUsed = System.nanoTime();
            try {
                return executor.submit(callable).get();
            } catch (RejectedExecutionException ex) {
                throw new JenaTransactionException("Remote transaction has ended or expired", ex);
            } catch (ExecutionException ex) {
                Throwable cause = ex.getCause();
                if ( cause instanceof Exception e )
                    throw e;
                if ( cause instanceof Error err )
                    throw err;
                throw ex;
            } finally {
                lastUsed = System.nanoTime();
                inFlight.decrementAndGet();
            }
        }

        boolean isIdleFor(long nanos) {
            return inFlight.get() == 0 && System.nanoTime() - lastUsed > nanos;
        }

        /** Call on the session thread. */
        void addPendingPatchRows(List<RDF_Patch_Row> rows) {
            pendingPatchRows.addAll(rows);
        }

        void commit(PatchPublisher publisher) throws Exception {
            run(() -> {
                // Reserve while still holding the write lock so delivery follows commit order.
                PatchPublisher.Ticket ticket = pendingPatchRows.isEmpty() ? null : publisher.reserve();
                try {
                    dsg.commit();
                } catch (Throwable ex) {
                    pendingPatchRows.clear();
                    if ( ticket != null )
                        ticket.cancel();
                    throw ex;
                }
                if ( ticket != null ) {
                    ticket.publish(List.copyOf(pendingPatchRows));
                    pendingPatchRows.clear();
                }
            });
        }

        void abort() throws Exception {
            run(() -> {
                pendingPatchRows.clear();
                dsg.abort();
            });
        }

        void end() throws Exception {
            if ( !ended ) {
                ended = true;
                try {
                    run(() -> {
                        pendingPatchRows.clear();
                        if ( dsg.isInTransaction() && dsg.transactionMode() == ReadWrite.WRITE ) {
                            dsg.abort();
                        }
                        dsg.end();
                    });
                } finally {
                    executor.shutdownNow();
                }
            }
        }

        void close() {
            try {
                end();
            } catch (Exception ignored) {
                executor.shutdownNow();
            }
        }
    }
}
