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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

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
import org.apache.jena.graph.Triple;
import org.apache.jena.grpc.wire.PB_RDFService.ApplyPatchRequest;
import org.apache.jena.grpc.wire.PB_RDFService.ApplyPatchResponse;
import org.apache.jena.grpc.wire.PB_RDFService.ExecuteQueryRequest;
import org.apache.jena.grpc.wire.PB_RDFService.ExecuteQueryResponse;
import org.apache.jena.grpc.wire.PB_RDFService.ExecuteUpdateRequest;
import org.apache.jena.grpc.wire.PB_RDFService.ExecuteUpdateResponse;
import org.apache.jena.grpc.wire.PB_RDFService.QueryResultForm;
import org.apache.jena.grpc.wire.PB_RDFService.SelectRowBatch;
import org.apache.jena.grpc.wire.PB_RDFService.StreamRdfRequest;
import org.apache.jena.grpc.wire.PB_RDFService.StreamRdfResponse;
import org.apache.jena.grpc.wire.PB_RDFService.SubscribePatchesRequest;
import org.apache.jena.grpc.wire.PB_RDFService.SubscribePatchesResponse;
import org.apache.jena.grpc.wire.PB_RDFService.TransactionCommand;
import org.apache.jena.grpc.wire.PB_RDFService.TransactionRequest;
import org.apache.jena.grpc.wire.PB_RDFService.TransactionResponse;
import org.apache.jena.grpc.wire.PB_RDFService.WriteDisposition;
import org.apache.jena.grpc.wire.PB_RDFService.WriteRdfHeader;
import org.apache.jena.grpc.wire.PB_RDFService.WriteRdfRequest;
import org.apache.jena.grpc.wire.PB_RDFService.WriteRdfResponse;
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
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_DataTuple;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Patch;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Patch_Row;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Quad;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Stream;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_StreamRow;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.riot.system.StreamRDFOps;
import org.apache.jena.riot.system.StreamRDFWrapper;
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
 * Stream-first gRPC {@link BindableService} exposing a Jena {@link DatasetGraph} over
 * {@code PB_RDF} and {@code PB_RDFService} messages without Jetty or Servlet dependencies.
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
     * @param batchSize number of rows per streamed batch message
     * @throws IllegalArgumentException Thrown if {@code dsg} is {@code null} or {@code batchSize <= 0}
     */
    public RDFDatasetGrpcService(DatasetGraph dsg, boolean encodeValues, int batchSize) {
        this(dsg, encodeValues, batchSize, DEFAULT_TXN_IDLE_TIMEOUT);
    }

    /**
     * Create a gRPC service backed by {@code dsg} with configurable value encoding, batch size,
     * and remote transaction idle timeout.
     *
     * @param dsg the backing {@link DatasetGraph}
     * @param encodeValues {@code true} to encode numeric literals by value in {@code PB_RDF} terms
     * @param batchSize number of rows per streamed batch message
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
            .addMethod(RDFGrpcProtocol.STREAM_RDF_METHOD,
                ServerCalls.asyncServerStreamingCall(this::streamRdf))
            .addMethod(RDFGrpcProtocol.WRITE_RDF_METHOD,
                ServerCalls.asyncClientStreamingCall(this::writeRdf))
            .addMethod(RDFGrpcProtocol.EXECUTE_QUERY_METHOD,
                ServerCalls.asyncServerStreamingCall(this::executeQuery))
            .addMethod(RDFGrpcProtocol.EXECUTE_UPDATE_METHOD,
                ServerCalls.asyncClientStreamingCall(this::executeUpdate))
            .addMethod(RDFGrpcProtocol.APPLY_PATCH_METHOD,
                ServerCalls.asyncClientStreamingCall(this::applyPatch))
            .addMethod(RDFGrpcProtocol.SUBSCRIBE_PATCHES_METHOD,
                ServerCalls.asyncServerStreamingCall(this::subscribePatches))
            .addMethod(RDFGrpcProtocol.TRANSACTION_METHOD,
                ServerCalls.asyncBidiStreamingCall(this::transaction))
            .build();
        return ServerInterceptors.intercept(raw, metadataInterceptor());
    }

    /**
     * Broadcast an {@link RDFPatch} to all active patch subscribers.
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

    /** Number of open remote transactions (package-private for tests). */
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
                return Contexts.interceptCall(ctx, call, headers, next);
            }
        };
    }

    private String resolveTxnId(String requestTxnId) {
        if ( requestTxnId != null && !requestTxnId.isEmpty() ) {
            return requestTxnId;
        }
        return RDFGrpcProtocol.CTX_TXN_ID.get();
    }

    private void streamRdf(StreamRdfRequest request, StreamObserver<StreamRdfResponse> responseObserver) {
        String txnId = resolveTxnId(request.getTxnId());
        try {
            executeRead(txnId, () -> {
                StreamRDF encoder = ProtobufRDF.streamToBatchConsumer(
                    batch -> responseObserver.onNext(
                        StreamRdfResponse.newBuilder().addAllRows(batch.getRowList()).build()),
                    batchSize,
                    encodeValues);
                encoder.start();
                switch ( request.getSelectorCase() ) {
                    case QUAD_PATTERN -> {
                        Quad pattern = ProtobufConvert.convert(request.getQuadPattern());
                        Node g = normalizePatternNode(pattern.getGraph());
                        Node s = normalizePatternNode(pattern.getSubject());
                        Node p = normalizePatternNode(pattern.getPredicate());
                        Node o = normalizePatternNode(pattern.getObject());
                        Iterator<Quad> iter = dsg.find(g, s, p, o);
                        while ( iter.hasNext() ) {
                            if ( isCancelled(responseObserver) )
                                break;
                            encoder.quad(iter.next());
                        }
                    }
                    case GRAPH -> {
                        Node graphName = RDFGrpcProtocol.decodeGraphTarget(request.getGraph());
                        Graph graph = (graphName == null) ? dsg.getDefaultGraph() : dsg.getGraph(graphName);
                        if ( graph != null ) {
                            StreamRDFOps.sendGraphToStream(graph, encoder);
                        }
                    }
                    case ENTIRE_DATASET, SELECTOR_NOT_SET -> StreamRDFOps.sendDatasetToStream(dsg, encoder);
                }
                encoder.finish();
            });
            responseObserver.onCompleted();
        } catch (Throwable ex) {
            responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
        }
    }

    /**
     * True O(1)-memory streaming RDF write handler. Opens or attaches to a single-thread write
     * session on the first frame and streams every incoming batch or row directly into the target
     * {@link Graph} or {@link DatasetGraph} as it arrives.
     */
    private StreamObserver<WriteRdfRequest> writeRdf(StreamObserver<WriteRdfResponse> responseObserver) {
        String headerTxnId = RDFGrpcProtocol.CTX_TXN_ID.get();
        return new StreamObserver<>() {
            private ServerTxnSession writeSession;
            private boolean ownsSession;
            private StreamRDF dest;
            private Consumer<RDF_StreamRow> rowSink;
            private Consumer<RDF_Stream> batchSink;
            private final AtomicLong quadCount = new AtomicLong();
            private final AtomicLong prefixCount = new AtomicLong();
            private boolean failed;

            private void ensureInitialized(WriteRdfHeader header) throws Exception {
                if ( writeSession != null ) {
                    return;
                }
                String effectiveTxnId = (header != null && !header.getTxnId().isEmpty())
                    ? header.getTxnId()
                    : headerTxnId;
                if ( effectiveTxnId != null ) {
                    writeSession = requireSession(effectiveTxnId);
                    ownsSession = false;
                } else {
                    writeSession = new ServerTxnSession(dsg, TxnType.WRITE);
                    ownsSession = true;
                }

                WriteDisposition disp = (header != null && header.getDisposition() != WriteDisposition.WRITE_DISPOSITION_UNSPECIFIED)
                    ? header.getDisposition()
                    : WriteDisposition.WRITE_DISPOSITION_APPEND;
                boolean targetGraph = header != null && header.getTargetCase() == WriteRdfHeader.TargetCase.GRAPH;
                Node graphName = targetGraph ? RDFGrpcProtocol.decodeGraphTarget(header.getGraph()) : null;

                writeSession.run(() -> {
                    if ( disp == WriteDisposition.WRITE_DISPOSITION_CLEAR ) {
                        if ( targetGraph ) {
                            if ( graphName == null ) {
                                dsg.getDefaultGraph().clear();
                            } else {
                                dsg.removeGraph(graphName);
                            }
                        } else {
                            dsg.clear();
                        }
                        return;
                    }
                    if ( disp == WriteDisposition.WRITE_DISPOSITION_REPLACE ) {
                        if ( targetGraph ) {
                            Graph g = (graphName == null) ? dsg.getDefaultGraph() : dsg.getGraph(graphName);
                            g.clear();
                        } else {
                            dsg.clear();
                        }
                    }
                    StreamRDF rawDest = targetGraph
                        ? StreamRDFLib.graph((graphName == null) ? dsg.getDefaultGraph() : dsg.getGraph(graphName))
                        : StreamRDFLib.dataset(dsg);
                    dest = new StreamRDFWrapper(rawDest) {
                        @Override
                        public void triple(Triple triple) {
                            quadCount.incrementAndGet();
                            super.triple(triple);
                        }

                        @Override
                        public void quad(Quad quad) {
                            quadCount.incrementAndGet();
                            super.quad(quad);
                        }

                        @Override
                        public void prefix(String prefix, String iri) {
                            prefixCount.incrementAndGet();
                            super.prefix(prefix, iri);
                        }
                    };
                    rowSink = ProtobufRDF.rowConsumerToStreamRDF(dest);
                    batchSink = ProtobufRDF.batchConsumerToStreamRDF(dest);
                    dest.start();
                });
            }

            @Override
            public void onNext(WriteRdfRequest request) {
                if ( failed )
                    return;
                try {
                    switch ( request.getItemCase() ) {
                        case HEADER -> ensureInitialized(request.getHeader());
                        case BATCH -> {
                            ensureInitialized(null);
                            if ( batchSink != null ) {
                                RDF_Stream batch = request.getBatch();
                                writeSession.run(() -> batchSink.accept(batch));
                            }
                        }
                        case ROW -> {
                            ensureInitialized(null);
                            if ( rowSink != null ) {
                                RDF_StreamRow row = request.getRow();
                                writeSession.run(() -> rowSink.accept(row));
                            }
                        }
                        case ITEM_NOT_SET -> { }
                    }
                } catch (Throwable ex) {
                    failed = true;
                    cleanupOnFailure();
                    responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
                }
            }

            @Override
            public void onError(Throwable t) {
                failed = true;
                cleanupOnFailure();
            }

            @Override
            public void onCompleted() {
                if ( failed )
                    return;
                try {
                    ensureInitialized(null);
                    if ( dest != null ) {
                        writeSession.run(dest::finish);
                    }
                    if ( ownsSession ) {
                        writeSession.commit(patchPublisher);
                        writeSession.close();
                    }
                    responseObserver.onNext(WriteRdfResponse.newBuilder()
                        .setQuadCount(quadCount.get())
                        .setPrefixCount(prefixCount.get())
                        .build());
                    responseObserver.onCompleted();
                } catch (Throwable ex) {
                    cleanupOnFailure();
                    responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
                }
            }

            private void cleanupOnFailure() {
                if ( ownsSession && writeSession != null ) {
                    try {
                        writeSession.abort();
                    } catch (Exception ignored) {
                    } finally {
                        writeSession.close();
                    }
                }
            }
        };
    }

    private void executeQuery(ExecuteQueryRequest request, StreamObserver<ExecuteQueryResponse> responseObserver) {
        String txnId = resolveTxnId(request.getTxnId());
        try {
            String baseUri = request.getBaseUri().isEmpty() ? null : request.getBaseUri();
            Query query = QueryFactory.create(request.getQuery(), baseUri, Syntax.syntaxARQ);
            for ( String uri : request.getDefaultGraphUrisList() ) {
                query.addGraphURI(uri);
            }
            for ( String uri : request.getNamedGraphUrisList() ) {
                query.addNamedGraphURI(uri);
            }
            QueryResultForm form = request.getResultForm();
            executeRead(txnId, () -> {
                try ( QueryExec qExec = QueryExec.dataset(dsg).query(query).build() ) {
                    if ( form == QueryResultForm.QUERY_RESULT_FORM_SELECT || query.isSelectType() ) {
                        streamSelectQuery(qExec, responseObserver);
                    } else if ( form == QueryResultForm.QUERY_RESULT_FORM_ASK || query.isAskType() ) {
                        boolean ans = qExec.ask();
                        responseObserver.onNext(ExecuteQueryResponse.newBuilder().setAskResult(ans).build());
                    } else if ( form == QueryResultForm.QUERY_RESULT_FORM_JSON || query.isJsonType() ) {
                        JsonArray array = qExec.execJson();
                        responseObserver.onNext(ExecuteQueryResponse.newBuilder()
                            .setJsonResult(JSON.toString(array))
                            .build());
                    } else if ( form == QueryResultForm.QUERY_RESULT_FORM_DATASET || query.isConstructQuad() ) {
                        streamDatasetQuery(query, qExec, responseObserver);
                    } else {
                        streamGraphQuery(query, qExec, responseObserver);
                    }
                }
            });
            responseObserver.onCompleted();
        } catch (Throwable ex) {
            responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
        }
    }

    private void streamSelectQuery(QueryExec qExec, StreamObserver<ExecuteQueryResponse> responseObserver) {
        RowSet rs = qExec.select();
        List<RDF_DataTuple> rowBuffer = new ArrayList<>(batchSize);
        ProtobufRDF.writeRowSet(
            rs,
            hdr -> responseObserver.onNext(ExecuteQueryResponse.newBuilder().setSelectHeader(hdr).build()),
            row -> {
                rowBuffer.add(row);
                if ( rowBuffer.size() >= batchSize ) {
                    flushSelectBatch(rowBuffer, responseObserver);
                }
            },
            encodeValues);
        if ( !rowBuffer.isEmpty() ) {
            flushSelectBatch(rowBuffer, responseObserver);
        }
    }

    private static void flushSelectBatch(
            List<RDF_DataTuple> rowBuffer, StreamObserver<ExecuteQueryResponse> responseObserver) {
        SelectRowBatch batch = SelectRowBatch.newBuilder().addAllRows(rowBuffer).build();
        rowBuffer.clear();
        responseObserver.onNext(ExecuteQueryResponse.newBuilder().setSelectRows(batch).build());
    }

    /**
     * Streams CONSTRUCT or DESCRIBE triples directly from {@link QueryExec#constructTriples()}
     * or {@link QueryExec#describeTriples()} without materializing an in-memory {@link Graph}.
     */
    private void streamGraphQuery(Query query, QueryExec qExec, StreamObserver<ExecuteQueryResponse> responseObserver) {
        StreamRDF encoder = ProtobufRDF.streamToBatchConsumer(
            batch -> responseObserver.onNext(ExecuteQueryResponse.newBuilder().setRdfBatch(batch).build()),
            batchSize,
            encodeValues);
        encoder.start();
        query.getPrefixMapping().getNsPrefixMap().forEach(encoder::prefix);
        Iterator<Triple> iter = query.isDescribeType() ? qExec.describeTriples() : qExec.constructTriples();
        while ( iter.hasNext() ) {
            if ( isCancelled(responseObserver) )
                break;
            encoder.triple(iter.next());
        }
        encoder.finish();
    }

    /**
     * Streams CONSTRUCT dataset quads directly from {@link QueryExec#constructQuads()}
     * without materializing an in-memory {@link DatasetGraph}.
     */
    private void streamDatasetQuery(Query query, QueryExec qExec, StreamObserver<ExecuteQueryResponse> responseObserver) {
        StreamRDF encoder = ProtobufRDF.streamToBatchConsumer(
            batch -> responseObserver.onNext(ExecuteQueryResponse.newBuilder().setRdfBatch(batch).build()),
            batchSize,
            encodeValues);
        encoder.start();
        query.getPrefixMapping().getNsPrefixMap().forEach(encoder::prefix);
        Iterator<Quad> iter = qExec.constructQuads();
        while ( iter.hasNext() ) {
            if ( isCancelled(responseObserver) )
                break;
            encoder.quad(iter.next());
        }
        encoder.finish();
    }

    private StreamObserver<ExecuteUpdateRequest> executeUpdate(StreamObserver<ExecuteUpdateResponse> responseObserver) {
        String headerTxnId = RDFGrpcProtocol.CTX_TXN_ID.get();
        return new StreamObserver<>() {
            private ServerTxnSession writeSession;
            private boolean ownsSession;
            private long updateCount;
            private boolean failed;

            private void ensureSession(String reqTxnId) throws Exception {
                if ( writeSession != null )
                    return;
                String effectiveTxnId = (reqTxnId != null && !reqTxnId.isEmpty()) ? reqTxnId : headerTxnId;
                if ( effectiveTxnId != null ) {
                    writeSession = requireSession(effectiveTxnId);
                    ownsSession = false;
                } else {
                    writeSession = new ServerTxnSession(dsg, TxnType.WRITE);
                    ownsSession = true;
                }
            }

            @Override
            public void onNext(ExecuteUpdateRequest request) {
                if ( failed )
                    return;
                try {
                    String baseUri = request.getBaseUri().isEmpty() ? null : request.getBaseUri();
                    UpdateRequest updateReq = UpdateFactory.create(request.getUpdate(), baseUri, Syntax.syntaxARQ);
                    ensureSession(request.getTxnId());
                    writeSession.run(() -> UpdateExec.dataset(dsg).update(updateReq).execute());
                    updateCount++;
                } catch (Throwable ex) {
                    failed = true;
                    cleanupOnFailure();
                    responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
                }
            }

            @Override
            public void onError(Throwable t) {
                failed = true;
                cleanupOnFailure();
            }

            @Override
            public void onCompleted() {
                if ( failed )
                    return;
                try {
                    if ( ownsSession && writeSession != null ) {
                        writeSession.commit(patchPublisher);
                        writeSession.close();
                    }
                    responseObserver.onNext(ExecuteUpdateResponse.newBuilder()
                        .setUpdateCount(updateCount)
                        .build());
                    responseObserver.onCompleted();
                } catch (Throwable ex) {
                    cleanupOnFailure();
                    responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
                }
            }

            private void cleanupOnFailure() {
                if ( ownsSession && writeSession != null ) {
                    try {
                        writeSession.abort();
                    } catch (Exception ignored) {
                    } finally {
                        writeSession.close();
                    }
                }
            }
        };
    }

    /**
     * True O(1)-memory streaming RDF Patch handler. Applies incoming {@link RDF_Patch_Row}
     * messages directly on the write transaction thread as each {@link ApplyPatchRequest}
     * arrives instead of buffering the entire stream in memory.
     */
    private StreamObserver<ApplyPatchRequest> applyPatch(StreamObserver<ApplyPatchResponse> responseObserver) {
        String headerTxnId = RDFGrpcProtocol.CTX_TXN_ID.get();
        return new StreamObserver<>() {
            private ServerTxnSession writeSession;
            private boolean ownsSession;
            private NonTransactionalChangesApply changes;
            private Consumer<RDF_Patch_Row> sink;
            private long rowCount;
            private boolean failed;

            private void ensureInitialized(String reqTxnId) throws Exception {
                if ( writeSession != null )
                    return;
                String effectiveTxnId = (reqTxnId != null && !reqTxnId.isEmpty()) ? reqTxnId : headerTxnId;
                if ( effectiveTxnId != null ) {
                    writeSession = requireSession(effectiveTxnId);
                    ownsSession = false;
                } else {
                    writeSession = new ServerTxnSession(dsg, TxnType.WRITE);
                    ownsSession = true;
                }
                writeSession.run(() -> {
                    changes = new NonTransactionalChangesApply(dsg);
                    sink = RDFPatchOps.protobufChangesReceiver(changes);
                    changes.start();
                });
            }

            @Override
            public void onNext(ApplyPatchRequest request) {
                if ( failed )
                    return;
                try {
                    ensureInitialized(request.getTxnId());
                    List<RDF_Patch_Row> rows = request.getRowsList();
                    if ( !rows.isEmpty() ) {
                        boolean captureForSubscribers = patchPublisher.hasSubscribers();
                        writeSession.run(() -> {
                            for ( RDF_Patch_Row row : rows ) {
                                sink.accept(row);
                            }
                            if ( captureForSubscribers ) {
                                writeSession.addPendingPatchRows(rows);
                            }
                        });
                        rowCount += rows.size();
                    }
                } catch (Throwable ex) {
                    failed = true;
                    cleanupOnFailure();
                    responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
                }
            }

            @Override
            public void onError(Throwable t) {
                failed = true;
                cleanupOnFailure();
            }

            @Override
            public void onCompleted() {
                if ( failed )
                    return;
                try {
                    if ( writeSession != null ) {
                        writeSession.run(() -> {
                            if ( changes != null ) {
                                changes.finish();
                            }
                        });
                        if ( ownsSession ) {
                            writeSession.commit(patchPublisher);
                            writeSession.close();
                        }
                    }
                    responseObserver.onNext(ApplyPatchResponse.newBuilder().setRowCount(rowCount).build());
                    responseObserver.onCompleted();
                } catch (Throwable ex) {
                    cleanupOnFailure();
                    responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
                }
            }

            private void cleanupOnFailure() {
                if ( ownsSession && writeSession != null ) {
                    try {
                        writeSession.abort();
                    } catch (Exception ignored) {
                    } finally {
                        writeSession.close();
                    }
                }
            }
        };
    }

    private void subscribePatches(
            SubscribePatchesRequest request, StreamObserver<SubscribePatchesResponse> responseObserver) {
        if ( responseObserver instanceof ServerCallStreamObserver<SubscribePatchesResponse> serverObs ) {
            serverObs.setOnCancelHandler(() -> patchPublisher.unsubscribe(responseObserver));
        }
        patchPublisher.subscribe(responseObserver);
    }

    /**
     * Bidirectional streaming transaction session. The lifetime of the transaction is bound
     * to the stream: if the client disconnects or cancels the stream before committing,
     * the server immediately aborts and ends the transaction.
     */
    private StreamObserver<TransactionRequest> transaction(StreamObserver<TransactionResponse> responseObserver) {
        return new StreamObserver<>() {
            private String sessionTxnId;
            private ServerTxnSession session;

            {
                if ( responseObserver instanceof ServerCallStreamObserver<TransactionResponse> serverObs ) {
                    serverObs.setOnCancelHandler(this::closeBoundSession);
                }
            }

            @Override
            public void onNext(TransactionRequest request) {
                try {
                    TransactionCommand cmd = request.getCommand();
                    switch ( cmd ) {
                        case TRANSACTION_COMMAND_BEGIN -> {
                            synchronized (this) {
                                if ( session != null ) {
                                    throw new JenaTransactionException("Transaction already active on this stream");
                                }
                                TxnType txnType = RDFGrpcProtocol.decodeTxnType(request.getTxnType());
                                String id = UUID.randomUUID().toString();
                                ServerTxnSession created = new ServerTxnSession(dsg, txnType);
                                activeTxns.put(id, created);
                                this.sessionTxnId = id;
                                this.session = created;
                            }
                            responseObserver.onNext(TransactionResponse.newBuilder()
                                .setCommand(TransactionCommand.TRANSACTION_COMMAND_BEGIN)
                                .setTxnId(sessionTxnId)
                                .build());
                        }
                        case TRANSACTION_COMMAND_PROMOTE -> {
                            ServerTxnSession current = currentOrThrow();
                            Promote promoteMode = RDFGrpcProtocol.decodePromoteMode(request.getPromoteMode());
                            boolean promoted = current.call(() -> dsg.promote(promoteMode));
                            responseObserver.onNext(TransactionResponse.newBuilder()
                                .setCommand(TransactionCommand.TRANSACTION_COMMAND_PROMOTE)
                                .setTxnId(sessionTxnId)
                                .setPromoted(promoted)
                                .build());
                        }
                        case TRANSACTION_COMMAND_COMMIT -> {
                            String committedId = sessionTxnId;
                            ServerTxnSession current = currentOrThrow();
                            current.commit(patchPublisher);
                            closeBoundSession();
                            responseObserver.onNext(TransactionResponse.newBuilder()
                                .setCommand(TransactionCommand.TRANSACTION_COMMAND_COMMIT)
                                .setTxnId(committedId)
                                .build());
                            responseObserver.onCompleted();
                        }
                        case TRANSACTION_COMMAND_ABORT -> {
                            String abortedId = sessionTxnId;
                            ServerTxnSession current = currentOrThrow();
                            current.abort();
                            closeBoundSession();
                            responseObserver.onNext(TransactionResponse.newBuilder()
                                .setCommand(TransactionCommand.TRANSACTION_COMMAND_ABORT)
                                .setTxnId(abortedId)
                                .build());
                            responseObserver.onCompleted();
                        }
                        case TRANSACTION_COMMAND_END -> {
                            String endedId = sessionTxnId != null ? sessionTxnId : "";
                            closeBoundSession();
                            responseObserver.onNext(TransactionResponse.newBuilder()
                                .setCommand(TransactionCommand.TRANSACTION_COMMAND_END)
                                .setTxnId(endedId)
                                .build());
                            responseObserver.onCompleted();
                        }
                        default -> throw new JenaTransactionException("Unsupported transaction command: " + cmd);
                    }
                } catch (Throwable ex) {
                    closeBoundSession();
                    responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
                }
            }

            @Override
            public void onError(Throwable t) {
                closeBoundSession();
            }

            @Override
            public void onCompleted() {
                closeBoundSession();
                responseObserver.onCompleted();
            }

            private synchronized ServerTxnSession currentOrThrow() {
                if ( sessionTxnId == null ) {
                    throw new JenaTransactionException("No active transaction on this stream");
                }
                return requireSession(sessionTxnId);
            }

            private synchronized void closeBoundSession() {
                String id = sessionTxnId;
                ServerTxnSession s = session;
                sessionTxnId = null;
                session = null;
                if ( id != null ) {
                    activeTxns.remove(id, s);
                }
                if ( s != null ) {
                    s.close();
                }
            }
        };
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

    private static boolean isCancelled(StreamObserver<?> observer) {
        return observer instanceof ServerCallStreamObserver<?> scso && scso.isCancelled();
    }

    private Node normalizePatternNode(Node node) {
        return (node == null) ? Node.ANY : node;
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
            // Enclosing write session manages the transaction lifecycle
        }

        @Override
        public void txnCommit() {
            // Enclosing write session manages the transaction lifecycle
        }

        @Override
        public void txnAbort() {
            throw new JenaTransactionException("Patch stream requested transaction abort (TA)");
        }
    }

    /**
     * Delivers committed patch batches to subscribers in commit order.
     */
    private static final class PatchPublisher {
        private final List<StreamObserver<SubscribePatchesResponse>> subscribers = new ArrayList<>();
        private final ArrayDeque<Ticket> tickets = new ArrayDeque<>();

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

        synchronized void subscribe(StreamObserver<SubscribePatchesResponse> subscriber) {
            subscribers.add(subscriber);
            send(subscriber, SubscribePatchesResponse.newBuilder().setSubscribed(true).build());
        }

        synchronized void unsubscribe(StreamObserver<SubscribePatchesResponse> subscriber) {
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
            RDF_Patch patch = RDF_Patch.newBuilder().addAllRow(patchRows).build();
            SubscribePatchesResponse msg = SubscribePatchesResponse.newBuilder().setPatch(patch).build();
            for ( StreamObserver<SubscribePatchesResponse> sub : List.copyOf(subscribers) ) {
                send(sub, msg);
            }
        }

        private boolean send(StreamObserver<SubscribePatchesResponse> sub, SubscribePatchesResponse msg) {
            try {
                sub.onNext(msg);
                return true;
            } catch (RuntimeException ex) {
                subscribers.remove(sub);
                return false;
            }
        }

        synchronized void close() {
            tickets.clear();
            for ( StreamObserver<SubscribePatchesResponse> sub : subscribers ) {
                try {
                    sub.onCompleted();
                } catch (RuntimeException ignored) {
                }
            }
            subscribers.clear();
        }
    }

    /**
     * Pins a server-side {@link DatasetGraph} transaction to a dedicated single-thread executor
     * so multiple gRPC requests sharing a transaction ID execute on the same thread.
     */
    private static final class ServerTxnSession {
        private final DatasetGraph dsg;
        private final ExecutorService executor;
        private final AtomicInteger inFlight = new AtomicInteger();
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

        void addPendingPatchRows(List<RDF_Patch_Row> rows) {
            pendingPatchRows.addAll(rows);
        }

        void commit(PatchPublisher publisher) throws Exception {
            run(() -> {
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
