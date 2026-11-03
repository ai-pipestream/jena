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
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonArray;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.grpc.RDFGrpcFrames.GetRdfFrame;
import org.apache.jena.grpc.RDFGrpcFrames.PutRdfFrame;
import org.apache.jena.grpc.RDFGrpcFrames.SessionRequest;
import org.apache.jena.grpc.RDFGrpcFrames.SessionResponse;
import org.apache.jena.grpc.RDFGrpcFrames.StreamRdfFrame;
import org.apache.jena.grpc.RDFGrpcFrames.WriteRdfFrame;
import org.apache.jena.grpc.wire.PB_RDFService.ApplyPatchRequest;
import org.apache.jena.grpc.wire.PB_RDFService.ApplyPatchResponse;
import org.apache.jena.grpc.wire.PB_RDFService.ExecuteQueryRequest;
import org.apache.jena.grpc.wire.PB_RDFService.ExecuteQueryResponse;
import org.apache.jena.grpc.wire.PB_RDFService.ExecuteUpdateRequest;
import org.apache.jena.grpc.wire.PB_RDFService.ExecuteUpdateResponse;
import org.apache.jena.grpc.wire.PB_RDFService.GetRdfRequest;
import org.apache.jena.grpc.wire.PB_RDFService.PutRdfResponse;
import org.apache.jena.grpc.wire.PB_RDFService.GraphTarget;
import org.apache.jena.grpc.wire.PB_RDFService.QueryResultForm;
import org.apache.jena.grpc.wire.PB_RDFService.SelectRowBatch;
import org.apache.jena.grpc.wire.PB_RDFService.StreamRdfRequest;
import org.apache.jena.grpc.wire.PB_RDFService.SubscribePatchesRequest;
import org.apache.jena.grpc.wire.PB_RDFService.SubscribePatchesResponse;
import org.apache.jena.grpc.wire.PB_RDFService.TransactionCommand;
import org.apache.jena.grpc.wire.PB_RDFService.TransactionRequest;
import org.apache.jena.grpc.wire.PB_RDFService.TransactionResponse;
import org.apache.jena.grpc.wire.PB_RDFService.WriteDisposition;
import org.apache.jena.grpc.wire.PB_RDFService.WriteRdfHeader;
import org.apache.jena.grpc.wire.PB_RDFService.WriteRdfResponse;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.Syntax;
import org.apache.jena.query.TxnType;
import org.apache.jena.rdfpatch.RDFChanges;
import org.apache.jena.rdfpatch.RDFPatch;
import org.apache.jena.rdfpatch.RDFPatchOps;
import org.apache.jena.rdfpatch.binary.RDFChangesWriterProtobuf;
import org.apache.jena.rdfpatch.changes.RDFChangesApply;
import org.apache.jena.rdfpatch.changes.RDFChangesWrapper;
import org.apache.jena.rdfpatch.system.DatasetGraphChanges;
import org.apache.jena.riot.protobuf.ProtobufConvert;
import org.apache.jena.riot.protobuf.ProtobufRDF;
import org.apache.jena.riot.protobuf.wire.PB_RDF.PatchTxn;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_DataTuple;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Patch;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Patch_Row;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Quad;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Stream;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_StreamRow;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Term;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapWrapper;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.riot.system.StreamRDFOps;
import org.apache.jena.riot.system.StreamRDFWrapper;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.JenaTransactionException;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Transactional.Promote;
import org.apache.jena.sparql.exec.QueryExec;
import org.apache.jena.sparql.exec.RowSet;
import org.apache.jena.sparql.exec.UpdateExec;
import org.apache.jena.sparql.modify.UsingList;
import org.apache.jena.system.Txn;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;

/**
 * Stream-first gRPC {@link BindableService} exposing a Jena {@link DatasetGraph} over
 * {@code PB_RDF} and {@code PB_RDFService} messages without Jetty or Servlet dependencies.
 * <p>
 * Every write, whether it arrives through {@code WriteRdf}, {@code ExecuteUpdate},
 * {@code ApplyPatch} or a remote {@code Transaction}, runs in a server-side write session.
 * When a session opens while at least one {@code SubscribePatches} client is registered, the
 * session records every change it applies and publishes them as one RDF Patch when it commits,
 * so a subscriber receives whole patches, in commit order, for every transaction that began
 * after the subscription was confirmed. Nothing is published for aborted sessions.
 * <p>
 * Sessions that stay idle longer than the configured idle timeout, including the sessions
 * opened by a non-transactional write call whose client has stalled, are aborted by a reaper
 * and the client call is failed with {@link Status#ABORTED}.
 */
public class RDFDatasetGrpcService implements BindableService, AutoCloseable {

    private static final String ERR_DSG_NULL = "dsg must not be null";
    private static final String ERR_PATCH_NULL = "patch must not be null";
    private static final int DEFAULT_BATCH_SIZE = ProtobufRDF.DEFAULT_BATCH_SIZE;

    /** Default time a remote transaction may stay idle before the server aborts and ends it. */
    public static final Duration DEFAULT_TXN_IDLE_TIMEOUT = Duration.ofMinutes(5);

    private final DatasetGraph dsg;
    private final boolean encodeValues;
    // Version 2 of the RDF Protobuf encoding: term tables and repeats.
    private final boolean termTables;
    private final int batchSize;
    private final long txnIdleTimeoutNanos;
    private final Map<String, ServerTxnSession> activeTxns = new ConcurrentHashMap<>();
    private final Set<ServerTxnSession> ownedWriteSessions = ConcurrentHashMap.newKeySet();
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
     * and write session idle timeout.
     *
     * @param dsg the backing {@link DatasetGraph}
     * @param encodeValues {@code true} to encode numeric literals by value in {@code PB_RDF} terms
     * @param batchSize number of rows per streamed batch message
     * @param txnIdleTimeout maximum idle time of a remote transaction or of a non-transactional
     *     streaming write call between two client frames
     * @throws IllegalArgumentException Thrown if {@code dsg} or {@code txnIdleTimeout} is {@code null},
     *     {@code batchSize <= 0}, or {@code txnIdleTimeout} is not positive
     */
    public RDFDatasetGrpcService(DatasetGraph dsg, boolean encodeValues, int batchSize, Duration txnIdleTimeout) {
        this(dsg, encodeValues, true, batchSize, txnIdleTimeout);
    }

    /**
     * Create a gRPC service backed by {@code dsg}.
     *
     * @param dsg the backing {@link DatasetGraph}
     * @param encodeValues {@code true} to encode numeric literals by value in {@code PB_RDF} terms
     * @param termTables {@code true} to send RDF with version 2 of the RDF Protobuf encoding
     *     (per-stream term tables and repeats, much smaller and faster to decode);
     *     {@code false} to write every term in full as version 1 does
     * @param batchSize number of rows per streamed batch message
     * @param txnIdleTimeout maximum idle time of a remote transaction or of a non-transactional
     *     streaming write call between two client frames
     * @throws IllegalArgumentException Thrown if {@code dsg} or {@code txnIdleTimeout} is {@code null},
     *     {@code batchSize <= 0}, or {@code txnIdleTimeout} is not positive
     */
    public RDFDatasetGrpcService(DatasetGraph dsg, boolean encodeValues, boolean termTables, int batchSize, Duration txnIdleTimeout) {
        if ( dsg == null )
            throw new IllegalArgumentException(ERR_DSG_NULL);
        if ( batchSize <= 0 )
            throw new IllegalArgumentException("batchSize must be positive: " + batchSize);
        if ( txnIdleTimeout == null || txnIdleTimeout.isNegative() || txnIdleTimeout.isZero() )
            throw new IllegalArgumentException("txnIdleTimeout must be positive: " + txnIdleTimeout);
        this.dsg = dsg;
        this.encodeValues = encodeValues;
        this.termTables = termTables;
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
            .addMethod(RDFGrpcProtocol.GET_RDF_METHOD,
                ServerCalls.asyncUnaryCall(this::getRdf))
            .addMethod(RDFGrpcProtocol.PUT_RDF_METHOD,
                ServerCalls.asyncUnaryCall(this::putRdf))
            .addMethod(RDFGrpcProtocol.RDF_SESSION_METHOD,
                ServerCalls.asyncBidiStreamingCall(this::rdfSession))
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
        for ( ServerTxnSession session : new ArrayList<>(ownedWriteSessions) ) {
            if ( ownedWriteSessions.remove(session) ) {
                session.close();
            }
        }
        patchPublisher.close();
    }

    /** Number of open remote transactions (package-private for tests). */
    int activeTxnCount() {
        return activeTxns.size();
    }

    /** Number of open write sessions owned by non-transactional write calls (package-private for tests). */
    int ownedWriteCount() {
        return ownedWriteSessions.size();
    }

    private void reapIdleTxns() {
        for ( Map.Entry<String, ServerTxnSession> e : activeTxns.entrySet() ) {
            ServerTxnSession session = e.getValue();
            if ( session.isIdleFor(txnIdleTimeoutNanos) && activeTxns.remove(e.getKey(), session) ) {
                Log.warn(this, "Remote transaction idle timeout, aborting: " + e.getKey());
                session.expire();
            }
        }
        for ( ServerTxnSession session : ownedWriteSessions ) {
            if ( session.isIdleFor(txnIdleTimeoutNanos) && ownedWriteSessions.remove(session) ) {
                Log.warn(this, "Streaming write call idle timeout, aborting");
                session.expire();
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

    private void streamRdf(StreamRdfRequest request, StreamObserver<StreamRdfFrame> responseObserver) {
        String txnId = resolveTxnId(request.getTxnId());
        try {
            executeRead(txnId, () -> {
                StreamRDF encoder = ProtobufRDF.streamToFrames(
                    frame -> responseObserver.onNext(new StreamRdfFrame(frame)),
                    batchSize,
                    encodeValues,
                    termTables);
                sendRdf(request, encoder, responseObserver);
            });
            responseObserver.onCompleted();
        } catch (Throwable ex) {
            responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
        }
    }

    /**
     * Unary read: the whole selection is encoded into one response message. Convenient for
     * small graphs; for anything large, {@code StreamRdf} or {@code RdfSession} keep the
     * memory of both sides bounded by the batch size.
     */
    private void getRdf(GetRdfRequest request, StreamObserver<GetRdfFrame> responseObserver) {
        StreamRdfRequest selection = request.getSelection();
        String txnId = resolveTxnId(selection.getTxnId());
        try {
            // One frame of unbounded size, handed over at finish().
            ByteBuffer[] rows = { ByteBuffer.allocate(0) };
            executeRead(txnId, () -> {
                StreamRDF encoder = ProtobufRDF.streamToFrames(frame -> rows[0] = frame, Integer.MAX_VALUE, encodeValues, termTables);
                sendRdf(selection, encoder, responseObserver);
            });
            responseObserver.onNext(new GetRdfFrame(rows[0]));
            responseObserver.onCompleted();
        } catch (Throwable ex) {
            responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
        }
    }

    /**
     * Send the selection of {@code request} to {@code encoder}. Must run inside a read
     * transaction. Stops early, without error, when the call has been cancelled.
     */
    private void sendRdf(StreamRdfRequest request, StreamRDF encoder, StreamObserver<?> call) {
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
                    if ( isCancelled(call) )
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
    }

    /** Unary write: the header and every row arrive in one message and go through the {@code WriteRdf} path. */
    private void putRdf(PutRdfFrame request, StreamObserver<PutRdfResponse> responseObserver) {
        StreamObserver<WriteRdfFrame> write = writeRdf(new StreamObserver<WriteRdfResponse>() {
            @Override
            public void onNext(WriteRdfResponse value) {
                responseObserver.onNext(PutRdfResponse.newBuilder()
                    .setQuadCount(value.getQuadCount())
                    .setPrefixCount(value.getPrefixCount())
                    .build());
            }

            @Override
            public void onError(Throwable t) {
                responseObserver.onError(t);
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        });
        write.onNext(WriteRdfFrame.header(request.header()));
        if ( request.rows() != null )
            write.onNext(WriteRdfFrame.rows(request.rows()));
        write.onCompleted();
    }

    /**
     * Bidirectional session: many reads and writes over one stream, each tagged with the
     * client's operation id. A read runs inline when its frame arrives and its rows go back
     * as {@code read_rows} events followed by {@code read_end}. A write opens a
     * {@code WriteRdf} call of its own and stays open until its {@code write_end} frame; its
     * summary comes back as {@code write_done}. One write is open at a time, because a write
     * without a remote transaction holds the dataset's write lock until it ends, but the
     * client need not wait for {@code write_done} before starting the next one, so a run of
     * writes is pipelined. A failed operation fails the stream and rolls back a write still
     * open on it.
     */
    private StreamObserver<SessionRequest> rdfSession(StreamObserver<SessionResponse> responseObserver) {
        return new StreamObserver<>() {
            // The writes of the session run one after another on one thread, rather than
            // starting a thread per write, which is most of the cost of a small write.
            private final ExecutorService writeExecutor = txnExecutor("jena-grpc-session");
            private StreamObserver<WriteRdfFrame> write = null;
            private long writeOpId = 0;
            private boolean terminated = false;

            @Override
            public synchronized void onNext(SessionRequest request) {
                if ( terminated )
                    return;
                long opId = request.opId();
                try {
                    if ( request.read() != null ) {
                        read(opId, request.read());
                    } else if ( request.write() != null ) {
                        if ( write != null )
                            throw Status.INVALID_ARGUMENT
                                .withDescription("Write " + writeOpId + " is still open on this session; end it before starting write " + opId)
                                .asRuntimeException();
                        write = writeRdf(writeEvents(opId), writeExecutor);
                        writeOpId = opId;
                        write.onNext(WriteRdfFrame.header(request.write()));
                    } else if ( request.writeRows() != null ) {
                        openWrite(opId).onNext(WriteRdfFrame.rows(request.writeRows()));
                    } else if ( request.writeEnd() ) {
                        StreamObserver<WriteRdfFrame> ending = openWrite(opId);
                        write = null;
                        ending.onCompleted();
                    } else {
                        throw Status.INVALID_ARGUMENT
                            .withDescription("Session frame without an operation")
                            .asRuntimeException();
                    }
                } catch (Throwable ex) {
                    fail(ex);
                }
            }

            @Override
            public synchronized void onError(Throwable t) {
                if ( terminated )
                    return;
                terminated = true;
                abortWrite();
                writeExecutor.shutdownNow();
            }

            @Override
            public synchronized void onCompleted() {
                if ( terminated )
                    return;
                if ( write != null ) {
                    fail(Status.INVALID_ARGUMENT
                        .withDescription("Write " + writeOpId + " is still open at the end of the session")
                        .asRuntimeException());
                    return;
                }
                terminated = true;
                writeExecutor.shutdownNow();
                responseObserver.onCompleted();
            }

            private void read(long opId, StreamRdfRequest request) throws Exception {
                String txnId = resolveTxnId(request.getTxnId());
                executeRead(txnId, () -> {
                    StreamRDF encoder = ProtobufRDF.streamToFrames(
                        frame -> send(SessionResponse.readRows(opId, frame)),
                        batchSize,
                        encodeValues,
                        termTables);
                    sendRdf(request, encoder, responseObserver);
                });
                send(SessionResponse.readEnd(opId));
            }

            private StreamObserver<WriteRdfFrame> openWrite(long opId) {
                if ( write == null || writeOpId != opId )
                    throw Status.INVALID_ARGUMENT
                        .withDescription("No open write " + opId + " on this session")
                        .asRuntimeException();
                return write;
            }

            private StreamObserver<WriteRdfResponse> writeEvents(long opId) {
                return new StreamObserver<>() {
                    @Override
                    public void onNext(WriteRdfResponse value) {
                        send(SessionResponse.writeDone(opId, value));
                    }

                    @Override
                    public void onError(Throwable t) {
                        fail(t);
                    }

                    @Override
                    public void onCompleted() {}
                };
            }

            private synchronized void send(SessionResponse response) {
                if ( !terminated )
                    responseObserver.onNext(response);
            }

            private synchronized void fail(Throwable ex) {
                if ( terminated )
                    return;
                terminated = true;
                abortWrite();
                writeExecutor.shutdownNow();
                responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
            }

            private void abortWrite() {
                StreamObserver<WriteRdfFrame> open = write;
                write = null;
                if ( open != null )
                    open.onError(Status.CANCELLED.withDescription("Session ended").asRuntimeException());
            }
        };
    }

    /**
     * Streaming RDF write handler. Opens or attaches to a single-thread write session on the first
     * frame and streams every incoming batch or row directly into the target {@link Graph} or
     * {@link DatasetGraph} as it arrives. Patch subscriber recording buffers committed changes.
     */
    private StreamObserver<WriteRdfFrame> writeRdf(StreamObserver<WriteRdfResponse> responseObserver) {
        return writeRdf(responseObserver, null);
    }

    /** As {@link #writeRdf(StreamObserver)}, running an owned write session on {@code sharedExecutor}. */
    private StreamObserver<WriteRdfFrame> writeRdf(StreamObserver<WriteRdfResponse> responseObserver, ExecutorService sharedExecutor) {
        return new WriteCall<>(responseObserver, sharedExecutor) {
            private StreamRDF dest;
            private Consumer<ByteBuffer> frameSink;
            private final AtomicLong quadCount = new AtomicLong();
            private final AtomicLong prefixCount = new AtomicLong();

            private void ensureInitialized(WriteRdfHeader header) throws Exception {
                String reqTxnId = (header != null) ? header.getTxnId() : null;
                if ( !ensureSession(reqTxnId) ) {
                    return;
                }
                WriteDisposition disp = (header != null && header.getDisposition() != WriteDisposition.WRITE_DISPOSITION_UNSPECIFIED)
                    ? header.getDisposition()
                    : WriteDisposition.WRITE_DISPOSITION_APPEND;
                boolean targetGraph = header != null && header.getTargetCase() == WriteRdfHeader.TargetCase.GRAPH;
                Node graphName = targetGraph ? RDFGrpcProtocol.decodeGraphTarget(header.getGraph()) : null;

                // Queued behind begin(); dest and frameSink are only touched on the transaction thread.
                session.submit(() -> {
                    DatasetGraph target = session.dataset();
                    if ( disp == WriteDisposition.WRITE_DISPOSITION_CLEAR ) {
                        if ( targetGraph ) {
                            if ( graphName == null ) {
                                target.getDefaultGraph().clear();
                            } else {
                                target.removeGraph(graphName);
                            }
                        } else {
                            target.clear();
                        }
                        return;
                    }
                    if ( disp == WriteDisposition.WRITE_DISPOSITION_REPLACE ) {
                        if ( targetGraph ) {
                            Graph g = (graphName == null) ? target.getDefaultGraph() : target.getGraph(graphName);
                            g.clear();
                        } else {
                            target.clear();
                        }
                    }
                    StreamRDF rawDest = targetGraph
                        ? StreamRDFLib.graph((graphName == null) ? target.getDefaultGraph() : target.getGraph(graphName))
                        : StreamRDFLib.dataset(target);
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
                    frameSink = ProtobufRDF.framesToStreamRDF(dest);
                    dest.start();
                });
            }

            @Override
            protected void handle(WriteRdfFrame request) throws Exception {
                if ( request.header() != null ) {
                    ensureInitialized(request.header());
                    return;
                }
                ensureInitialized(null);
                ByteBuffer rows = request.rows();
                session.submit(() -> {
                    if ( frameSink != null )
                        frameSink.accept(rows);
                });
            }

            @Override
            protected WriteRdfResponse finish() throws Exception {
                ensureInitialized(null);
                session.run(() -> {
                    if ( dest != null )
                        dest.finish();
                });
                return WriteRdfResponse.newBuilder()
                    .setQuadCount(quadCount.get())
                    .setPrefixCount(prefixCount.get())
                    .build();
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
            encodeValues,
            termTables);
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
            encodeValues,
            termTables);
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
        return new WriteCall<>(responseObserver) {
            private long updateCount;

            @Override
            protected void handle(ExecuteUpdateRequest request) throws Exception {
                String baseUri = request.getBaseUri().isEmpty() ? null : request.getBaseUri();
                UpdateRequest updateReq = UpdateFactory.create(request.getUpdate(), baseUri, Syntax.syntaxARQ);
                UsingList usingList = new UsingList();
                for ( String uri : request.getUsingGraphUrisList() ) {
                    usingList.addUsing(NodeFactory.createURI(uri));
                }
                for ( String uri : request.getUsingNamedGraphUrisList() ) {
                    usingList.addUsingNamed(NodeFactory.createURI(uri));
                }
                UpdateRequest update = UsingList.modifyUpdateForUsingList(updateReq, usingList);
                ensureSession(request.getTxnId());
                session.run(() -> UpdateExec.dataset(session.dataset()).update(update).execute());
                updateCount++;
            }

            @Override
            protected ExecuteUpdateResponse finish() {
                return ExecuteUpdateResponse.newBuilder()
                    .setUpdateCount(updateCount)
                    .build();
            }
        };
    }

    /**
     * Streaming RDF Patch handler. Applies incoming {@link RDF_Patch_Row} messages directly on
     * the write transaction thread as each {@link ApplyPatchRequest} arrives instead of buffering
     * the request. When a subscriber requires change recording, committed changes are buffered so
     * each subscriber receives the whole patch or nothing.
     */
    private StreamObserver<ApplyPatchRequest> applyPatch(StreamObserver<ApplyPatchResponse> responseObserver) {
        return new WriteCall<>(responseObserver) {
            private RDFChanges changes;
            private Consumer<RDF_Patch_Row> sink;
            private long rowCount;

            private void ensureInitialized(String reqTxnId) throws Exception {
                if ( !ensureSession(reqTxnId) ) {
                    return;
                }
                session.run(() -> {
                    changes = new NonTransactionalChangesApply(session);
                    sink = RDFPatchOps.protobufChangesReceiver(changes);
                    changes.start();
                });
            }

            @Override
            protected void handle(ApplyPatchRequest request) throws Exception {
                ensureInitialized(request.getTxnId());
                List<RDF_Patch_Row> rows = request.getRowsList();
                if ( !rows.isEmpty() ) {
                    session.run(() -> {
                        for ( RDF_Patch_Row row : rows ) {
                            sink.accept(row);
                        }
                    });
                    rowCount += rows.size();
                }
            }

            @Override
            protected ApplyPatchResponse finish() throws Exception {
                if ( session != null && changes != null ) {
                    session.run(changes::finish);
                }
                return ApplyPatchResponse.newBuilder().setRowCount(rowCount).build();
            }
        };
    }

    private void subscribePatches(
            SubscribePatchesRequest request, StreamObserver<SubscribePatchesResponse> responseObserver) {
        GraphFilter filter;
        try {
            filter = GraphFilter.of(request.hasGraphFilter() ? request.getGraphFilter() : null);
        } catch (RuntimeException ex) {
            responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
            return;
        }
        if ( responseObserver instanceof ServerCallStreamObserver<SubscribePatchesResponse> serverObs ) {
            serverObs.setOnCancelHandler(() -> patchPublisher.unsubscribe(responseObserver));
        }
        patchPublisher.subscribe(responseObserver, filter);
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
                                ServerTxnSession created = openSession(txnType, null);
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

    /**
     * Open a new server-side session. The session records changes for patch subscribers when it
     * may write and at least one subscriber is registered at the time it opens.
     */
    /** Open a remote transaction; returns once it has begun. */
    private ServerTxnSession openSession(TxnType txnType, Runnable onExpire) throws Exception {
        boolean record = txnType != TxnType.READ && patchPublisher.hasSubscribers();
        return new ServerTxnSession(dsg, txnType, record, onExpire, null, true);
    }

    /** Open the session of a write call; the begin is queued and the call waits at its end. */
    private ServerTxnSession openWriteCallSession(Runnable onExpire, ExecutorService sharedExecutor) throws Exception {
        boolean record = patchPublisher.hasSubscribers();
        return new ServerTxnSession(dsg, TxnType.WRITE, record, onExpire, sharedExecutor, false);
    }

    /** A single thread that transactions are bound to; daemon, so it never keeps a JVM alive. */
    private static ExecutorService txnExecutor(String name) {
        return Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        });
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
     * Shared lifecycle of the three client-streaming write calls ({@code WriteRdf},
     * {@code ExecuteUpdate} and {@code ApplyPatch}). The call attaches to the remote
     * transaction named by the request or the call metadata, or opens a write session of its
     * own that is committed when the client half-closes the stream. An owned session is
     * registered with the idle reaper; when the reaper expires it, the call fails with
     * {@link Status#ABORTED} and the partial write is rolled back.
     */
    private abstract class WriteCall<Req, Resp> implements StreamObserver<Req> {
        private final StreamObserver<Resp> responseObserver;
        private final String headerTxnId = RDFGrpcProtocol.CTX_TXN_ID.get();
        private final AtomicBoolean terminated = new AtomicBoolean();
        // Thread for an owned session, shared by the writes of one RdfSession stream; null for a thread per call.
        private final ExecutorService sharedExecutor;
        /** The write session of this call, {@code null} until the first frame. */
        protected ServerTxnSession session;
        private boolean ownsSession;

        WriteCall(StreamObserver<Resp> responseObserver) {
            this(responseObserver, null);
        }

        WriteCall(StreamObserver<Resp> responseObserver, ExecutorService sharedExecutor) {
            this.responseObserver = responseObserver;
            this.sharedExecutor = sharedExecutor;
        }

        /** Handle one request frame on the gRPC thread. */
        protected abstract void handle(Req request) throws Exception;

        /** Flush any pending work and build the response; called before the owned session commits. */
        protected abstract Resp finish() throws Exception;

        /**
         * Attach to the remote transaction or open an owned write session on the first frame.
         *
         * @return {@code true} if this call opened or attached the session now, {@code false} if it already had one
         */
        protected final boolean ensureSession(String reqTxnId) throws Exception {
            if ( session != null )
                return false;
            String effectiveTxnId = (reqTxnId != null && !reqTxnId.isEmpty()) ? reqTxnId : headerTxnId;
            if ( effectiveTxnId != null ) {
                session = requireSession(effectiveTxnId);
                ownsSession = false;
            } else {
                ServerTxnSession owned = openWriteCallSession(this::expire, sharedExecutor);
                ownedWriteSessions.add(owned);
                session = owned;
                ownsSession = true;
            }
            return true;
        }

        @Override
        public final void onNext(Req request) {
            if ( terminated.get() )
                return;
            try {
                handle(request);
            } catch (Throwable ex) {
                fail(ex);
            }
        }

        @Override
        public final void onError(Throwable t) {
            if ( terminated.compareAndSet(false, true) ) {
                releaseOwned(true);
            }
        }

        @Override
        public final void onCompleted() {
            if ( terminated.get() )
                return;
            try {
                Resp response = finish();
                if ( ownsSession ) {
                    session.commit(patchPublisher);
                    releaseOwned(false);
                }
                if ( terminated.compareAndSet(false, true) ) {
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                }
            } catch (Throwable ex) {
                fail(ex);
            }
        }

        private void fail(Throwable ex) {
            if ( !terminated.compareAndSet(false, true) )
                return;
            releaseOwned(true);
            responseObserver.onError(RDFGrpcProtocol.toStatusException(ex));
        }

        /** Called by the reaper, on the reaper thread, after it aborted and ended the owned session. */
        private void expire() {
            if ( !terminated.compareAndSet(false, true) )
                return;
            responseObserver.onError(Status.ABORTED
                .withDescription("Streaming write call idle timeout; the write was rolled back")
                .asRuntimeException());
        }

        private void releaseOwned(boolean abort) {
            if ( !ownsSession || session == null )
                return;
            ownedWriteSessions.remove(session);
            if ( abort ) {
                try {
                    session.abort();
                } catch (Exception ignored) {
                }
            }
            session.close();
        }
    }

    /**
     * Applies patch data and prefix operations within an already active write session.
     * Patch headers are passed to the session recorder so that subscribers see them.
     */
    private static final class NonTransactionalChangesApply extends RDFChangesApply {
        private final ServerTxnSession session;

        NonTransactionalChangesApply(ServerTxnSession session) {
            super(session.dataset());
            this.session = session;
        }

        @Override
        public void header(String field, Node value) {
            session.recordHeader(field, value);
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
     * Graph filter of a patch subscription. {@code ALL} delivers every patch; otherwise only the
     * rows of the selected graph are delivered, inside their {@code TX}/{@code TC} framing, and a
     * patch with no row in that graph is not delivered at all.
     */
    private record GraphFilter(boolean enabled, Node graphName) {
        static final GraphFilter ALL = new GraphFilter(false, null);

        static GraphFilter of(GraphTarget target) {
            if ( target == null || target.getTargetCase() == GraphTarget.TargetCase.TARGET_NOT_SET )
                return ALL;
            return new GraphFilter(true, RDFGrpcProtocol.decodeGraphTarget(target));
        }

        /**
         * Return the rows of {@code rows} that this filter delivers, or an empty list when no
         * data row of the patch matches.
         */
        List<RDF_Patch_Row> apply(List<RDF_Patch_Row> rows) {
            if ( !enabled )
                return rows;
            List<RDF_Patch_Row> out = new ArrayList<>(rows.size());
            boolean anyData = false;
            for ( RDF_Patch_Row row : rows ) {
                switch ( row.getRowCase() ) {
                    case HEADER, TXN, ROW_NOT_SET -> out.add(row);
                    case DATAADD -> {
                        if ( matches(row.getDataAdd().hasG() ? row.getDataAdd().getG() : null) ) {
                            out.add(row);
                            anyData = true;
                        }
                    }
                    case DATADEL -> {
                        if ( matches(row.getDataDel().hasG() ? row.getDataDel().getG() : null) ) {
                            out.add(row);
                            anyData = true;
                        }
                    }
                    case PREFIXADD -> {
                        if ( matches(row.getPrefixAdd().hasGraphNode() ? row.getPrefixAdd().getGraphNode() : null) ) {
                            out.add(row);
                            anyData = true;
                        }
                    }
                    case PREFIXDEL -> {
                        if ( matches(row.getPrefixDel().hasGraphNode() ? row.getPrefixDel().getGraphNode() : null) ) {
                            out.add(row);
                            anyData = true;
                        }
                    }
                }
            }
            return anyData ? out : List.of();
        }

        private boolean matches(RDF_Term graphTerm) {
            Node g = null;
            if ( graphTerm != null
                    && graphTerm.getTermCase() != RDF_Term.TermCase.TERM_NOT_SET
                    && !graphTerm.hasUndefined() ) {
                g = ProtobufConvert.convert(graphTerm);
                if ( g != null && Quad.isDefaultGraph(g) )
                    g = null;
            }
            return (graphName == null) ? g == null : graphName.equals(g);
        }
    }

    /**
     * Delivers committed patch batches to subscribers in commit order.
     */
    private static final class PatchPublisher {
        private record Subscriber(StreamObserver<SubscribePatchesResponse> observer, GraphFilter filter) {}

        private final List<Subscriber> subscribers = new ArrayList<>();
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

        synchronized void subscribe(StreamObserver<SubscribePatchesResponse> subscriber, GraphFilter filter) {
            subscribers.add(new Subscriber(subscriber, filter));
            send(subscriber, SubscribePatchesResponse.newBuilder().setSubscribed(true).build());
        }

        synchronized void unsubscribe(StreamObserver<SubscribePatchesResponse> subscriber) {
            subscribers.removeIf(s -> s.observer() == subscriber);
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
            SubscribePatchesResponse unfiltered = null;
            for ( Subscriber sub : List.copyOf(subscribers) ) {
                SubscribePatchesResponse msg;
                if ( sub.filter().enabled() ) {
                    List<RDF_Patch_Row> filtered = sub.filter().apply(patchRows);
                    if ( filtered.isEmpty() )
                        continue;
                    msg = toMessage(filtered);
                } else {
                    if ( unfiltered == null )
                        unfiltered = toMessage(patchRows);
                    msg = unfiltered;
                }
                send(sub.observer(), msg);
            }
        }

        private static SubscribePatchesResponse toMessage(List<RDF_Patch_Row> rows) {
            RDF_Patch patch = RDF_Patch.newBuilder().addAllRow(rows).build();
            return SubscribePatchesResponse.newBuilder().setPatch(patch).build();
        }

        private boolean send(StreamObserver<SubscribePatchesResponse> sub, SubscribePatchesResponse msg) {
            try {
                sub.onNext(msg);
                return true;
            } catch (RuntimeException ex) {
                unsubscribe(sub);
                return false;
            }
        }

        synchronized void close() {
            tickets.clear();
            for ( Subscriber sub : subscribers ) {
                try {
                    sub.observer().onCompleted();
                } catch (RuntimeException ignored) {
                }
            }
            subscribers.clear();
        }
    }

    /**
     * Pins a server-side {@link DatasetGraph} transaction to a dedicated single-thread executor
     * so multiple gRPC requests sharing a transaction ID execute on the same thread.
     * <p>
     * A recording session exposes, through {@link #dataset()}, a {@link DatasetGraphChanges}
     * view of the dataset that captures every add, delete and prefix change made through it.
     * On commit the captured rows are published as one patch, framed by {@code TX} and
     * {@code TC}; on abort they are dropped.
     */
    private static final class ServerTxnSession {
        private final DatasetGraph rawDsg;
        private final DatasetGraph view;
        private final ExecutorService executor;
        private final boolean ownsExecutor;
        private final AtomicInteger inFlight = new AtomicInteger();
        private final Runnable onExpire;
        private final List<RDF_Patch_Row> headerRows;
        private final List<RDF_Patch_Row> changeRows;
        private final RDFChanges recorder;
        private volatile long lastUsed = System.nanoTime();
        // The first failure of a queued step; rethrown by the next step and by every later one.
        private volatile Throwable failure = null;
        private boolean ended;

        /**
         * @param sharedExecutor the thread to run the transaction on, shared with other sessions
         *     that run one after another, or {@code null} for a thread of this session's own
         * @param awaitBegin {@code true} to return only once the transaction has begun, as a
         *     remote transaction must; {@code false} to queue the begin, so that a write call
         *     can queue its frames behind it and wait once, at the end
         */
        ServerTxnSession(DatasetGraph dsg, TxnType txnType, boolean record, Runnable onExpire,
                         ExecutorService sharedExecutor, boolean awaitBegin) throws Exception {
            this.rawDsg = dsg;
            this.onExpire = onExpire;
            if ( record ) {
                this.headerRows = new ArrayList<>();
                this.changeRows = new ArrayList<>();
                this.recorder = new DefaultGraphNormalizingChanges(RDFChangesWriterProtobuf.create(changeRows::add));
                this.view = new RecordingDatasetGraph(dsg, recorder);
            } else {
                this.headerRows = null;
                this.changeRows = null;
                this.recorder = null;
                this.view = dsg;
            }
            this.ownsExecutor = (sharedExecutor == null);
            this.executor = ownsExecutor ? txnExecutor("jena-grpc-txn") : sharedExecutor;
            if ( awaitBegin )
                run(() -> dsg.begin(txnType));
            else
                submit(() -> dsg.begin(txnType));
        }

        /** The dataset that operations of this session must write through. */
        DatasetGraph dataset() {
            return view;
        }

        void run(Runnable runnable) throws Exception {
            call(() -> {
                runnable.run();
                return null;
            });
        }

        /**
         * Queue a step on the transaction thread without waiting for it. Steps run in the
         * order queued; the thread is the same one {@link #run} and {@link #call} use, so a
         * later awaited step runs after every queued one. If a queued step fails, that step
         * and the ones after it are skipped and the failure is thrown by the next step that
         * is queued or awaited.
         */
        void submit(Runnable runnable) throws Exception {
            rethrowFailure();
            inFlight.incrementAndGet();
            lastUsed = System.nanoTime();
            try {
                executor.execute(() -> {
                    try {
                        if ( failure == null )
                            runnable.run();
                    } catch (Throwable t) {
                        if ( failure == null )
                            failure = t;
                    } finally {
                        lastUsed = System.nanoTime();
                        inFlight.decrementAndGet();
                    }
                });
            } catch (RejectedExecutionException ex) {
                inFlight.decrementAndGet();
                throw new JenaTransactionException("Remote transaction has ended or expired", ex);
            }
        }

        <T> T call(Callable<T> callable) throws Exception {
            rethrowFailure();
            inFlight.incrementAndGet();
            lastUsed = System.nanoTime();
            try {
                T result = executor.submit(callable).get();
                rethrowFailure();
                return result;
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

        private void rethrowFailure() throws Exception {
            Throwable t = failure;
            if ( t == null )
                return;
            if ( t instanceof Exception e )
                throw e;
            if ( t instanceof Error err )
                throw err;
            throw new RuntimeException(t);
        }

        boolean isIdleFor(long nanos) {
            return inFlight.get() == 0 && System.nanoTime() - lastUsed > nanos;
        }

        /** Record a patch header row for subscribers; must be called on the session thread. */
        void recordHeader(String field, Node value) {
            if ( recorder == null )
                return;
            RDFChanges headerWriter = RDFChangesWriterProtobuf.create(headerRows::add);
            headerWriter.header(field, value);
        }

        private List<RDF_Patch_Row> recordedPatch() {
            if ( changeRows == null || changeRows.isEmpty() )
                return List.of();
            List<RDF_Patch_Row> rows = new ArrayList<>(headerRows.size() + changeRows.size() + 2);
            rows.addAll(headerRows);
            rows.add(RDF_Patch_Row.newBuilder().setTxn(PatchTxn.TX).build());
            rows.addAll(changeRows);
            rows.add(RDF_Patch_Row.newBuilder().setTxn(PatchTxn.TC).build());
            return rows;
        }

        private void clearRecorded() {
            if ( recorder != null ) {
                headerRows.clear();
                changeRows.clear();
            }
        }

        void commit(PatchPublisher publisher) throws Exception {
            run(() -> {
                List<RDF_Patch_Row> rows = recordedPatch();
                PatchPublisher.Ticket ticket = rows.isEmpty() ? null : publisher.reserve();
                try {
                    rawDsg.commit();
                } catch (Throwable ex) {
                    clearRecorded();
                    if ( ticket != null )
                        ticket.cancel();
                    throw ex;
                }
                clearRecorded();
                if ( ticket != null ) {
                    ticket.publish(rows);
                }
            });
        }

        void abort() throws Exception {
            run(() -> {
                clearRecorded();
                rawDsg.abort();
            });
        }

        /** Abort and end the session because it was idle for too long, then notify the owner. */
        void expire() {
            close();
            if ( onExpire != null ) {
                onExpire.run();
            }
        }

        void end() throws Exception {
            synchronized (this) {
                if ( ended )
                    return;
                ended = true;
            }
            try {
                run(() -> {
                    clearRecorded();
                    if ( rawDsg.isInTransaction() && rawDsg.transactionMode() == ReadWrite.WRITE ) {
                        rawDsg.abort();
                    }
                    rawDsg.end();
                });
            } finally {
                if ( ownsExecutor )
                    executor.shutdownNow();
            }
        }

        void close() {
            try {
                end();
            } catch (Exception ignored) {
                if ( ownsExecutor )
                    executor.shutdownNow();
            }
        }
    }

    /**
     * Records default graph changes with no graph name, whichever default graph marker node
     * the writer used, so that subscribers receive standard RDF Patch default graph rows.
     */
    private static final class DefaultGraphNormalizingChanges extends RDFChangesWrapper {
        DefaultGraphNormalizingChanges(RDFChanges other) {
            super(other);
        }

        private static Node normalize(Node g) {
            return (g != null && Quad.isDefaultGraph(g)) ? null : g;
        }

        @Override
        public void add(Node g, Node s, Node p, Node o) {
            super.add(normalize(g), s, p, o);
        }

        @Override
        public void delete(Node g, Node s, Node p, Node o) {
            super.delete(normalize(g), s, p, o);
        }

        @Override
        public void addPrefix(Node gn, String prefix, String uriStr) {
            super.addPrefix(normalize(gn), prefix, uriStr);
        }

        @Override
        public void deletePrefix(Node gn, String prefix) {
            super.deletePrefix(normalize(gn), prefix);
        }
    }

    /**
     * {@link DatasetGraphChanges} that also records changes made through the dataset-level
     * {@link DatasetGraph#prefixes()} map and through {@link DatasetGraph#clear()}, which the
     * base class passes through unrecorded.
     */
    private static final class RecordingDatasetGraph extends DatasetGraphChanges {
        private final RDFChanges monitor;

        RecordingDatasetGraph(DatasetGraph dsg, RDFChanges monitor) {
            super(dsg, monitor);
            this.monitor = monitor;
        }

        @Override
        public PrefixMap prefixes() {
            return new PrefixMapWrapper(super.prefixes()) {
                @Override
                public void add(String prefix, String iri) {
                    monitor.addPrefix(null, prefix, iri);
                    super.add(prefix, iri);
                }

                @Override
                public void putAll(PrefixMap pmap) {
                    pmap.forEach(this::add);
                }

                @Override
                public void putAll(PrefixMapping pmap) {
                    pmap.getNsPrefixMap().forEach(this::add);
                }

                @Override
                public void putAll(Map<String, String> mapping) {
                    mapping.forEach(this::add);
                }

                @Override
                public void delete(String prefix) {
                    monitor.deletePrefix(null, prefix);
                    super.delete(prefix);
                }

                @Override
                public void clear() {
                    for ( String prefix : getMappingCopy().keySet() ) {
                        delete(prefix);
                    }
                }
            };
        }

        @Override
        public void clear() {
            deleteAny(Node.ANY, Node.ANY, Node.ANY, Node.ANY);
            prefixes().clear();
            super.clear();
        }
    }
}
