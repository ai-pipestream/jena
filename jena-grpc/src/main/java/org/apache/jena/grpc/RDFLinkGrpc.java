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
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.grpc.RDFGrpcFrames.StreamRdfFrame;
import org.apache.jena.grpc.RDFGrpcFrames.WriteRdfFrame;
import org.apache.jena.grpc.wire.PB_RDFService.ApplyPatchRequest;
import org.apache.jena.grpc.wire.PB_RDFService.StreamRdfRequest;
import org.apache.jena.grpc.wire.PB_RDFService.SubscribePatchesRequest;
import org.apache.jena.grpc.wire.PB_RDFService.SubscribePatchesResponse;
import org.apache.jena.grpc.wire.PB_RDFService.TransactionCommand;
import org.apache.jena.grpc.wire.PB_RDFService.TransactionRequest;
import org.apache.jena.grpc.wire.PB_RDFService.TransactionResponse;
import org.apache.jena.grpc.wire.PB_RDFService.WriteDisposition;
import org.apache.jena.grpc.wire.PB_RDFService.WriteRdfHeader;
import org.apache.jena.query.Query;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.rdflink.RDFLink;
import org.apache.jena.rdfpatch.RDFChanges;
import org.apache.jena.rdfpatch.RDFPatch;
import org.apache.jena.rdfpatch.RDFPatchOps;
import org.apache.jena.rdfpatch.binary.RDFChangesWriterProtobuf;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.protobuf.ProtobufRDF;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Patch;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Patch_Row;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Quad;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Stream;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFBase;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.riot.system.StreamRDFOps;
import org.apache.jena.sparql.ARQException;
import org.apache.jena.sparql.JenaTransactionException;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Transactional.Promote;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.exec.QueryExec;
import org.apache.jena.sparql.exec.QueryExecApp;
import org.apache.jena.sparql.exec.RowSet;
import org.apache.jena.sparql.graph.GraphFactory;
import org.apache.jena.system.Txn;
import org.apache.jena.update.Update;
import org.apache.jena.update.UpdateRequest;

/**
 * Jetty-free, stream-first gRPC implementation of {@link RDFLink} communicating over
 * {@code PB_RDF} and {@code PB_RDFService} Protobuf messages, supporting lazy streaming
 * quad pattern matching ({@link #findStream}), streaming dataset/graph ingest,
 * connection-bound bidirectional transactions, transactional {@link RDFPatch} application
 * and subscription, and SPARQL Query/Update.
 */
public class RDFLinkGrpc implements RDFLink {

    private static final String ERR_CLOSED = "RDFLinkGrpc is closed";
    private static final String ERR_PATCH_NULL = "patch must not be null";
    private static final String ERR_PRODUCER_NULL = "producer must not be null";
    private static final String ERR_TARGET_NULL = "target must not be null";
    private static final String ERR_DEST_NULL = "dest must not be null";
    private static final Object STREAM_EOF = new Object();

    private final Channel rawChannel;
    private final Channel interceptedChannel;
    private final boolean ownChannel;
    private final boolean parseCheckQueries;
    private final boolean parseCheckUpdates;
    private final boolean encodeValues;
    private final boolean termTables;
    private final int batchSize;
    private final Duration txnCommandTimeout;
    private final ThreadLocal<ClientTxnSession> currentTxn = new ThreadLocal<>();
    private volatile boolean open = true;

    RDFLinkGrpc(
            Channel channel,
            boolean ownChannel,
            boolean parseCheckQueries,
            boolean parseCheckUpdates,
            boolean encodeValues,
            boolean termTables,
            int batchSize,
            Duration txnCommandTimeout) {
        this.rawChannel = Objects.requireNonNull(channel);
        this.ownChannel = ownChannel;
        this.parseCheckQueries = parseCheckQueries;
        this.parseCheckUpdates = parseCheckUpdates;
        this.encodeValues = encodeValues;
        this.termTables = termTables;
        this.batchSize = batchSize;
        this.txnCommandTimeout = txnCommandTimeout;
        this.interceptedChannel = ClientInterceptors.intercept(rawChannel, txnHeaderInterceptor());
    }

    /**
     * Create a new {@link RDFLinkGrpcBuilder}.
     *
     * @return a new builder instance
     */
    public static RDFLinkGrpcBuilder newBuilder() {
        return new RDFLinkGrpcBuilder();
    }

    /**
     * Connect to a remote {@link RDFGrpcServer} over Netty TCP transport.
     *
     * @param host the server host
     * @param port the server TCP port
     * @return the connected {@link RDFLinkGrpc}
     * @throws IllegalArgumentException Thrown if {@code host} is {@code null} or {@code port <= 0}
     */
    public static RDFLinkGrpc connect(String host, int port) {
        return newBuilder().host(host).port(port).build();
    }

    /**
     * Connect to an in-process {@link RDFGrpcServer} by its server name.
     *
     * @param serverName the in-process server name
     * @return the connected {@link RDFLinkGrpc}
     * @throws IllegalArgumentException Thrown if {@code serverName} is {@code null} or blank
     */
    public static RDFLinkGrpc connectInProcess(String serverName) {
        return newBuilder().inProcessName(serverName).build();
    }

    /**
     * Wrap an existing gRPC {@link Channel} in an {@link RDFLinkGrpc} without taking ownership of channel shutdown.
     *
     * @param channel the gRPC channel
     * @return the {@link RDFLinkGrpc} wrapping {@code channel}
     * @throws IllegalArgumentException Thrown if {@code channel} is {@code null}
     */
    public static RDFLinkGrpc connect(Channel channel) {
        return newBuilder().channel(channel).ownChannel(false).build();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isRemote() {
        return true;
    }

    /**
     * Lazily stream quads matching {@code (g, s, p, o)} from the remote dataset using {@code RDF_ANY}
     * wildcards for {@code null} or {@link Node#ANY} components.
     * <p>
     * The returned {@link Stream} pulls chunks from the server on demand without buffering the
     * full result set in memory. Closing the returned {@link Stream} (for example via try-with-resources
     * after {@link Stream#findFirst()} or {@link Stream#limit(long)}) cancels the underlying gRPC call.
     *
     * @param g graph node pattern, or {@code null} / {@link Node#ANY} for any graph
     * @param s subject node pattern, or {@code null} / {@link Node#ANY} for any subject
     * @param p predicate node pattern, or {@code null} / {@link Node#ANY} for any predicate
     * @param o object node pattern, or {@code null} / {@link Node#ANY} for any object
     * @return a lazy {@link Stream} of matching {@link Quad} instances
     */
    public Stream<Quad> findStream(Node g, Node s, Node p, Node o) {
        checkOpen();
        RDF_Quad pattern = RDFGrpcProtocol.encodeQuadPattern(g, s, p, o);
        StreamRdfRequest request = StreamRdfRequest.newBuilder().setQuadPattern(pattern).build();
        ClientCall<StreamRdfRequest, StreamRdfFrame> call =
            interceptedChannel.newCall(RDFGrpcProtocol.STREAM_RDF_METHOD, CallOptions.DEFAULT);
        Iterator<StreamRdfFrame> responseIter = ClientCalls.blockingServerStreamingCall(call, request);
        ArrayDeque<Quad> buffer = new ArrayDeque<>();
        StreamRDF collector = new StreamRDFBase() {
            @Override
            public void triple(Triple triple) {
                buffer.addLast(Quad.create(Quad.defaultGraphIRI, triple));
            }

            @Override
            public void quad(Quad quad) {
                buffer.addLast(quad);
            }
        };
        Consumer<ByteBuffer> decoder = ProtobufRDF.framesToStreamRDF(collector);
        collector.start();

        Iterator<Quad> quadIter = new Iterator<>() {
            private boolean finished;

            @Override
            public boolean hasNext() {
                try {
                    while ( buffer.isEmpty() && !finished ) {
                        if ( !responseIter.hasNext() ) {
                            finished = true;
                            collector.finish();
                            return false;
                        }
                        decoder.accept(responseIter.next().rows());
                    }
                    return !buffer.isEmpty();
                } catch (StatusRuntimeException sre) {
                    throw RDFGrpcProtocol.fromStatusException(sre);
                }
            }

            @Override
            public Quad next() {
                if ( !hasNext() ) {
                    throw new NoSuchElementException();
                }
                return buffer.removeFirst();
            }
        };

        Spliterator<Quad> spliterator =
            Spliterators.spliteratorUnknownSize(quadIter, Spliterator.ORDERED | Spliterator.NONNULL);
        return StreamSupport.stream(spliterator, false)
            .onClose(() -> call.cancel("findStream closed by caller", null));
    }

    /**
     * Stream quads matching {@code (g, s, p, o)} from the remote dataset into {@code dest}.
     *
     * @param g graph node pattern, or {@code null} / {@link Node#ANY} for any graph
     * @param s subject node pattern, or {@code null} / {@link Node#ANY} for any subject
     * @param p predicate node pattern, or {@code null} / {@link Node#ANY} for any predicate
     * @param o object node pattern, or {@code null} / {@link Node#ANY} for any object
     * @param dest the destination {@link StreamRDF} sink
     * @throws IllegalArgumentException Thrown if {@code dest} is {@code null}
     */
    public void findStream(Node g, Node s, Node p, Node o, StreamRDF dest) {
        if ( dest == null )
            throw new IllegalArgumentException(ERR_DEST_NULL);
        checkOpen();
        RDF_Quad pattern = RDFGrpcProtocol.encodeQuadPattern(g, s, p, o);
        streamRdfToSink(StreamRdfRequest.newBuilder().setQuadPattern(pattern).build(), dest);
    }

    /**
     * Stream quads matching {@code (g, s, p, o)} from the remote dataset into {@code dest}
     * using batched {@code StreamRdf} frames.
     *
     * @param g graph node pattern, or {@code null} / {@link Node#ANY} for any graph
     * @param s subject node pattern, or {@code null} / {@link Node#ANY} for any subject
     * @param p predicate node pattern, or {@code null} / {@link Node#ANY} for any predicate
     * @param o object node pattern, or {@code null} / {@link Node#ANY} for any object
     * @param dest the destination {@link StreamRDF} sink
     * @throws IllegalArgumentException Thrown if {@code dest} is {@code null}
     */
    public void findStreamBatched(Node g, Node s, Node p, Node o, StreamRDF dest) {
        findStream(g, s, p, o, dest);
    }

    /**
     * Stream RDF triples and quads produced by {@code producer} into the remote dataset
     * without buffering the dataset in memory.
     *
     * @param producer callback that writes to the provided {@link StreamRDF}
     * @throws IllegalArgumentException Thrown if {@code producer} is {@code null}
     */
    public void streamLoadDataset(Consumer<StreamRDF> producer) {
        streamLoadDatasetBatched(producer, batchSize);
    }

    /**
     * Stream RDF triples and quads produced by {@code producer} into the remote dataset
     * as batched {@link RDF_Stream} messages.
     *
     * @param producer callback that writes to the provided {@link StreamRDF}
     * @param streamBatchSize maximum number of rows per {@link RDF_Stream} batch
     * @throws IllegalArgumentException Thrown if {@code producer} is {@code null} or {@code streamBatchSize <= 0}
     */
    public void streamLoadDatasetBatched(Consumer<StreamRDF> producer, int streamBatchSize) {
        if ( producer == null )
            throw new IllegalArgumentException(ERR_PRODUCER_NULL);
        if ( streamBatchSize <= 0 )
            throw new IllegalArgumentException("streamBatchSize must be positive: " + streamBatchSize);
        checkOpen();
        WriteRdfHeader header = WriteRdfHeader.newBuilder()
            .setEntireDataset(true)
            .setDisposition(WriteDisposition.WRITE_DISPOSITION_APPEND)
            .build();
        writeRdfStream(header, streamBatchSize, producer);
    }

    /**
     * Apply an {@link RDFPatch} to the remote dataset by streaming {@link ApplyPatchRequest} chunks.
     *
     * @param patch the patch to apply
     * @throws IllegalArgumentException Thrown if {@code patch} is {@code null}
     */
    public void applyPatch(RDFPatch patch) {
        if ( patch == null )
            throw new IllegalArgumentException(ERR_PATCH_NULL);
        applyPatch(patch::apply);
    }

    /**
     * Apply changes emitted to an {@link RDFChanges} sink to the remote dataset by streaming
     * bounded {@link ApplyPatchRequest} chunks in O(1) memory.
     *
     * @param producer callback that emits patch changes
     * @throws IllegalArgumentException Thrown if {@code producer} is {@code null}
     */
    public void applyPatch(Consumer<RDFChanges> producer) {
        if ( producer == null )
            throw new IllegalArgumentException(ERR_PRODUCER_NULL);
        checkOpen();
        executeClientStream(interceptedChannel, RDFGrpcProtocol.APPLY_PATCH_METHOD, requestObserver -> {
            List<RDF_Patch_Row> rowBuffer = new ArrayList<>(batchSize);
            RDFChanges writer = RDFPatchOps.protobufWriter(row -> {
                rowBuffer.add(row);
                if ( rowBuffer.size() >= batchSize ) {
                    requestObserver.onNext(ApplyPatchRequest.newBuilder().addAllRows(rowBuffer).build());
                    rowBuffer.clear();
                }
            });
            writer.start();
            try {
                producer.accept(writer);
            } finally {
                writer.finish();
                if ( !rowBuffer.isEmpty() ) {
                    requestObserver.onNext(ApplyPatchRequest.newBuilder().addAllRows(rowBuffer).build());
                    rowBuffer.clear();
                }
            }
        });
    }

    /**
     * Apply an {@link RDFPatch} to the remote dataset as a single batched {@link ApplyPatchRequest} frame.
     *
     * @param patch the patch to apply
     * @throws IllegalArgumentException Thrown if {@code patch} is {@code null}
     */
    public void applyPatchBatch(RDFPatch patch) {
        if ( patch == null )
            throw new IllegalArgumentException(ERR_PATCH_NULL);
        checkOpen();
        RDF_Patch batch = RDFChangesWriterProtobuf.toProtobuf(patch);
        executeClientStream(interceptedChannel, RDFGrpcProtocol.APPLY_PATCH_METHOD, requestObserver ->
            requestObserver.onNext(ApplyPatchRequest.newBuilder().addAllRows(batch.getRowList()).build()));
    }

    /**
     * Handle for an active patch subscription that does not throw a checked exception on {@link #close()}.
     */
    @FunctionalInterface
    public interface PatchSubscription extends AutoCloseable {
        @Override
        void close();

        /**
         * Return the terminal stream error if the subscription terminated abnormally, or {@code null}.
         *
         * @return the terminal error, or {@code null}
         */
        default Throwable error() {
            return null;
        }
    }

    /**
     * Subscribe to live {@link RDF_Patch_Row} messages published by the remote server,
     * decoding each row into {@code target}.
     *
     * @param target the destination {@link RDFChanges} sink
     * @return a {@link PatchSubscription} handle that cancels the subscription when closed
     * @throws IllegalArgumentException Thrown if {@code target} is {@code null}
     */
    public PatchSubscription subscribePatches(RDFChanges target) {
        return subscribePatches(target, t -> Log.warn(RDFLinkGrpc.class, "Patch subscription terminated with error", t));
    }

    /**
     * Subscribe to live {@link RDF_Patch_Row} messages published by the remote server,
     * decoding each row into {@code target} and notifying {@code errorHandler} if the stream fails.
     *
     * @param target the destination {@link RDFChanges} sink
     * @param errorHandler callback invoked on unexpected stream termination errors (may be {@code null})
     * @return a {@link PatchSubscription} handle that cancels the subscription when closed
     * @throws IllegalArgumentException Thrown if {@code target} is {@code null}
     */
    public PatchSubscription subscribePatches(RDFChanges target, Consumer<Throwable> errorHandler) {
        return subscribePatches(SubscribePatchesRequest.getDefaultInstance(), target, errorHandler);
    }

    /**
     * Subscribe to the committed changes of one graph. Only the rows of {@code graphName}
     * are delivered, inside their {@code TX}/{@code TC} framing; a commit that did not touch
     * the graph is not delivered at all.
     *
     * @param graphName the graph name, or {@code null} for the default graph
     * @param target the destination {@link RDFChanges} sink
     * @return a {@link PatchSubscription} handle that cancels the subscription when closed
     * @throws IllegalArgumentException Thrown if {@code target} is {@code null}
     */
    public PatchSubscription subscribePatchesForGraph(Node graphName, RDFChanges target) {
        return subscribePatchesForGraph(graphName, target,
            t -> Log.warn(RDFLinkGrpc.class, "Patch subscription terminated with error", t));
    }

    /**
     * Subscribe to the committed changes of one graph, notifying {@code errorHandler} if the
     * stream fails. Only the rows of {@code graphName} are delivered, inside their
     * {@code TX}/{@code TC} framing; a commit that did not touch the graph is not delivered at all.
     *
     * @param graphName the graph name, or {@code null} for the default graph
     * @param target the destination {@link RDFChanges} sink
     * @param errorHandler callback invoked on unexpected stream termination errors (may be {@code null})
     * @return a {@link PatchSubscription} handle that cancels the subscription when closed
     * @throws IllegalArgumentException Thrown if {@code target} is {@code null}
     */
    public PatchSubscription subscribePatchesForGraph(Node graphName, RDFChanges target, Consumer<Throwable> errorHandler) {
        SubscribePatchesRequest request = SubscribePatchesRequest.newBuilder()
            .setGraphFilter(RDFGrpcProtocol.encodeGraphTarget(graphName))
            .build();
        return subscribePatches(request, target, errorHandler);
    }

    private PatchSubscription subscribePatches(
            SubscribePatchesRequest request, RDFChanges target, Consumer<Throwable> errorHandler) {
        if ( target == null )
            throw new IllegalArgumentException(ERR_TARGET_NULL);
        checkOpen();
        ClientCall<SubscribePatchesRequest, SubscribePatchesResponse> call =
            interceptedChannel.newCall(RDFGrpcProtocol.SUBSCRIBE_PATCHES_METHOD, CallOptions.DEFAULT);
        Consumer<RDF_Patch_Row> sink = RDFPatchOps.protobufChangesReceiver(target);
        CountDownLatch readyLatch = new CountDownLatch(1);
        AtomicReference<Throwable> errorRef = new AtomicReference<>();
        ClientCalls.asyncServerStreamingCall(
            call,
            request,
            new StreamObserver<>() {
                @Override
                public void onNext(SubscribePatchesResponse value) {
                    readyLatch.countDown();
                    if ( value.hasPatch() ) {
                        for ( RDF_Patch_Row row : value.getPatch().getRowList() ) {
                            sink.accept(row);
                        }
                    }
                }

                @Override
                public void onError(Throwable t) {
                    readyLatch.countDown();
                    if ( t instanceof StatusRuntimeException sre
                            && sre.getStatus().getCode() == Status.Code.CANCELLED ) {
                        return;
                    }
                    errorRef.set(t);
                    if ( errorHandler != null ) {
                        errorHandler.accept(t);
                    }
                }

                @Override
                public void onCompleted() {
                    readyLatch.countDown();
                    target.finish();
                }
            });
        try {
            readyLatch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        return new PatchSubscription() {
            @Override
            public void close() {
                call.cancel("Subscription closed by client", null);
            }

            @Override
            public Throwable error() {
                return errorRef.get();
            }
        };
    }

    /** {@inheritDoc} */
    @Override
    public void queryRowSet(String queryString, Consumer<RowSet> rowSetAction) {
        Txn.executeRead(this, () -> {
            try ( QueryExec qExec = query(queryString) ) {
                rowSetAction.accept(qExec.select());
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public void querySelect(String queryString, Consumer<Binding> rowAction) {
        Txn.executeRead(this, () -> {
            try ( QueryExec qExec = query(queryString) ) {
                qExec.select().forEachRemaining(rowAction);
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public Graph queryConstruct(String queryString) {
        return Txn.calculateRead(this, () -> {
            try ( QueryExec qExec = query(queryString) ) {
                return qExec.construct();
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public Graph queryDescribe(String queryString) {
        return Txn.calculateRead(this, () -> {
            try ( QueryExec qExec = query(queryString) ) {
                return qExec.describe();
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public boolean queryAsk(String queryString) {
        return Txn.calculateRead(this, () -> {
            try ( QueryExec qExec = query(queryString) ) {
                return qExec.ask();
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public QueryExec query(Query query) {
        Objects.requireNonNull(query);
        checkOpen();
        QueryExecGrpcBuilder builder = newQuery();
        builder.query(query);
        return QueryExecApp.create(builder, null, query, null);
    }

    /** {@inheritDoc} */
    @Override
    public QueryExec query(String queryString) {
        Objects.requireNonNull(queryString);
        checkOpen();
        QueryExecGrpcBuilder builder = newQuery();
        builder.query(queryString);
        return QueryExecApp.create(builder, null, null, queryString);
    }

    /** {@inheritDoc} */
    @Override
    public QueryExecGrpcBuilder newQuery() {
        checkOpen();
        return new QueryExecGrpcBuilder(interceptedChannel, () -> CallOptions.DEFAULT, parseCheckQueries);
    }

    /** {@inheritDoc} */
    @Override
    public UpdateExecGrpcBuilder newUpdate() {
        checkOpen();
        return new UpdateExecGrpcBuilder(interceptedChannel, () -> CallOptions.DEFAULT, parseCheckUpdates);
    }

    /** {@inheritDoc} */
    @Override
    public void update(Update update) {
        Objects.requireNonNull(update);
        update(new UpdateRequest(update));
    }

    /** {@inheritDoc} */
    @Override
    public void update(UpdateRequest update) {
        Objects.requireNonNull(update);
        checkOpen();
        newUpdate().update(update).build().execute();
    }

    /** {@inheritDoc} */
    @Override
    public void update(String updateString) {
        Objects.requireNonNull(updateString);
        checkOpen();
        newUpdate().update(updateString).build().execute();
    }

    /** {@inheritDoc} */
    @Override
    public Graph get(Node graphName) {
        checkOpen();
        StreamRdfRequest request = StreamRdfRequest.newBuilder()
            .setGraph(RDFGrpcProtocol.encodeGraphTarget(graphName))
            .build();
        Graph graph = GraphFactory.createDefaultGraph();
        streamRdfToSink(request, StreamRDFLib.graph(graph));
        return graph;
    }

    /** {@inheritDoc} */
    @Override
    public Graph get() {
        return get((Node) null);
    }

    /** {@inheritDoc} */
    @Override
    public void load(Node graphName, String file) {
        Objects.requireNonNull(file);
        checkOpen();
        Graph g = RDFDataMgr.loadGraph(file);
        load(graphName, g);
    }

    /** {@inheritDoc} */
    @Override
    public void load(String file) {
        load(null, file);
    }

    /** {@inheritDoc} */
    @Override
    public void load(Node graphName, Graph graph) {
        Objects.requireNonNull(graph);
        checkOpen();
        WriteRdfHeader header = WriteRdfHeader.newBuilder()
            .setGraph(RDFGrpcProtocol.encodeGraphTarget(graphName))
            .setDisposition(WriteDisposition.WRITE_DISPOSITION_APPEND)
            .build();
        writeRdfStream(header, batchSize, encoder -> StreamRDFOps.sendGraphToStream(graph, encoder));
    }

    /** {@inheritDoc} */
    @Override
    public void load(Graph graph) {
        load(null, graph);
    }

    /** {@inheritDoc} */
    @Override
    public void put(Node graphName, String file) {
        Objects.requireNonNull(file);
        checkOpen();
        Graph g = RDFDataMgr.loadGraph(file);
        put(graphName, g);
    }

    /** {@inheritDoc} */
    @Override
    public void put(String file) {
        put(null, file);
    }

    /** {@inheritDoc} */
    @Override
    public void put(Node graphName, Graph graph) {
        Objects.requireNonNull(graph);
        checkOpen();
        WriteRdfHeader header = WriteRdfHeader.newBuilder()
            .setGraph(RDFGrpcProtocol.encodeGraphTarget(graphName))
            .setDisposition(WriteDisposition.WRITE_DISPOSITION_REPLACE)
            .build();
        writeRdfStream(header, batchSize, encoder -> StreamRDFOps.sendGraphToStream(graph, encoder));
    }

    /** {@inheritDoc} */
    @Override
    public void put(Graph graph) {
        put(null, graph);
    }

    /** {@inheritDoc} */
    @Override
    public void delete(Node graphName) {
        checkOpen();
        WriteRdfHeader header = WriteRdfHeader.newBuilder()
            .setGraph(RDFGrpcProtocol.encodeGraphTarget(graphName))
            .setDisposition(WriteDisposition.WRITE_DISPOSITION_CLEAR)
            .build();
        writeRdfStream(header, batchSize, null);
    }

    /** {@inheritDoc} */
    @Override
    public void delete() {
        delete(null);
    }

    /** {@inheritDoc} */
    @Override
    public DatasetGraph getDataset() {
        checkOpen();
        StreamRdfRequest request = StreamRdfRequest.newBuilder()
            .setEntireDataset(true)
            .build();
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
        streamRdfToSink(request, StreamRDFLib.dataset(dsg));
        return dsg;
    }

    /** {@inheritDoc} */
    @Override
    public void loadDataset(String file) {
        Objects.requireNonNull(file);
        checkOpen();
        streamLoadDataset(stream -> RDFDataMgr.parse(stream, file));
    }

    /** {@inheritDoc} */
    @Override
    public void loadDataset(DatasetGraph dataset) {
        Objects.requireNonNull(dataset);
        checkOpen();
        streamLoadDataset(stream -> StreamRDFOps.sendDatasetToStream(dataset, stream));
    }

    /** {@inheritDoc} */
    @Override
    public void putDataset(String file) {
        Objects.requireNonNull(file);
        checkOpen();
        WriteRdfHeader header = WriteRdfHeader.newBuilder()
            .setEntireDataset(true)
            .setDisposition(WriteDisposition.WRITE_DISPOSITION_REPLACE)
            .build();
        writeRdfStream(header, batchSize, encoder -> RDFDataMgr.parse(encoder, file));
    }

    /** {@inheritDoc} */
    @Override
    public void putDataset(DatasetGraph dataset) {
        Objects.requireNonNull(dataset);
        checkOpen();
        WriteRdfHeader header = WriteRdfHeader.newBuilder()
            .setEntireDataset(true)
            .setDisposition(WriteDisposition.WRITE_DISPOSITION_REPLACE)
            .build();
        writeRdfStream(header, batchSize, encoder -> StreamRDFOps.sendDatasetToStream(dataset, encoder));
    }

    /** {@inheritDoc} */
    @Override
    public void clearDataset() {
        checkOpen();
        WriteRdfHeader header = WriteRdfHeader.newBuilder()
            .setEntireDataset(true)
            .setDisposition(WriteDisposition.WRITE_DISPOSITION_CLEAR)
            .build();
        writeRdfStream(header, batchSize, null);
    }

    /** {@inheritDoc} */
    @Override
    public void begin() {
        begin(TxnType.WRITE);
    }

    /** {@inheritDoc} */
    @Override
    public void begin(TxnType txnType) {
        Objects.requireNonNull(txnType, "txnType must not be null");
        checkOpen();
        if ( isInTransaction() ) {
            throw new JenaTransactionException("Already in a transaction");
        }
        ClientTxnSession session = new ClientTxnSession(rawChannel, txnType, txnCommandTimeout);
        session.begin();
        currentTxn.set(session);
    }

    /** {@inheritDoc} */
    @Override
    public void begin(ReadWrite readWrite) {
        begin(TxnType.convert(readWrite));
    }

    /** {@inheritDoc} */
    @Override
    public boolean promote(Promote promote) {
        checkOpen();
        ClientTxnSession session = currentTxn.get();
        if ( session == null ) {
            throw new JenaTransactionException("Not in a transaction");
        }
        if ( session.txnMode == ReadWrite.WRITE ) {
            return true;
        }
        if ( session.txnType == TxnType.READ ) {
            return false;
        }
        Promote mode = (promote != null) ? promote : Promote.ISOLATED;
        TransactionResponse resp = session.sendCommand(TransactionRequest.newBuilder()
            .setCommand(TransactionCommand.TRANSACTION_COMMAND_PROMOTE)
            .setPromoteMode(RDFGrpcProtocol.encodePromoteMode(mode))
            .build());
        if ( resp.getPromoted() ) {
            session.txnMode = ReadWrite.WRITE;
            return true;
        }
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public void commit() {
        checkOpen();
        ClientTxnSession session = currentTxn.get();
        if ( session == null ) {
            throw new JenaTransactionException("Not in a transaction");
        }
        try {
            session.sendCommand(TransactionRequest.newBuilder()
                .setCommand(TransactionCommand.TRANSACTION_COMMAND_COMMIT)
                .build());
        } finally {
            currentTxn.remove();
            session.completeClientStream();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void abort() {
        checkOpen();
        ClientTxnSession session = currentTxn.get();
        if ( session == null ) {
            throw new JenaTransactionException("Not in a transaction");
        }
        try {
            session.sendCommand(TransactionRequest.newBuilder()
                .setCommand(TransactionCommand.TRANSACTION_COMMAND_ABORT)
                .build());
        } finally {
            currentTxn.remove();
            session.completeClientStream();
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean isInTransaction() {
        return currentTxn.get() != null;
    }

    /** {@inheritDoc} */
    @Override
    public void end() {
        ClientTxnSession session = currentTxn.get();
        currentTxn.remove();
        if ( session != null ) {
            ReadWrite mode = session.txnMode;
            session.endAndClose();
            if ( mode == ReadWrite.WRITE ) {
                throw new JenaTransactionException("Write transaction - no commit or abort before end()");
            }
        }
    }

    /**
     * Package-private helper for testing abrupt client stream cancellation during an active transaction.
     */
    void cancelTransactionStreamAbruptly() {
        ClientTxnSession session = currentTxn.get();
        currentTxn.remove();
        if ( session != null ) {
            session.cancelAbruptly();
        }
    }

    /** {@inheritDoc} */
    @Override
    public ReadWrite transactionMode() {
        ClientTxnSession session = currentTxn.get();
        return session != null ? session.txnMode : null;
    }

    /** {@inheritDoc} */
    @Override
    public TxnType transactionType() {
        ClientTxnSession session = currentTxn.get();
        return session != null ? session.txnType : null;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isClosed() {
        return !open;
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        if ( !open )
            return;
        open = false;
        ClientTxnSession session = currentTxn.get();
        currentTxn.remove();
        if ( session != null ) {
            session.endAndClose();
        }
        if ( ownChannel && rawChannel instanceof ManagedChannel mc ) {
            mc.shutdown();
            try {
                if ( !mc.awaitTermination(5, TimeUnit.SECONDS) ) {
                    mc.shutdownNow();
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                mc.shutdownNow();
            }
        }
    }

    private void checkOpen() {
        if ( !open )
            throw new ARQException(ERR_CLOSED);
    }

    private void streamRdfToSink(StreamRdfRequest request, StreamRDF dest) {
        try {
            Iterator<StreamRdfFrame> iter = ClientCalls.blockingServerStreamingCall(
                interceptedChannel, RDFGrpcProtocol.STREAM_RDF_METHOD, CallOptions.DEFAULT, request);
            Consumer<ByteBuffer> sink = ProtobufRDF.framesToStreamRDF(dest);
            dest.start();
            while ( iter.hasNext() ) {
                sink.accept(iter.next().rows());
            }
            dest.finish();
        } catch (StatusRuntimeException sre) {
            throw RDFGrpcProtocol.fromStatusException(sre);
        }
    }

    private void writeRdfStream(WriteRdfHeader header, int writeBatchSize, Consumer<StreamRDF> producer) {
        executeClientStream(interceptedChannel, RDFGrpcProtocol.WRITE_RDF_METHOD, requestObserver -> {
            requestObserver.onNext(WriteRdfFrame.header(header));
            if ( producer != null ) {
                StreamRDF encoder = ProtobufRDF.streamToFrames(
                    frame -> requestObserver.onNext(WriteRdfFrame.rows(frame)),
                    writeBatchSize,
                    encodeValues,
                    termTables);
                encoder.start();
                producer.accept(encoder);
                encoder.finish();
            }
        });
    }

    private ClientInterceptor txnHeaderInterceptor() {
        return new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                    MethodDescriptor<ReqT, RespT> method,
                    CallOptions callOptions,
                    Channel next) {
                return new SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        ClientTxnSession session = currentTxn.get();
                        if ( session != null && session.txnId != null ) {
                            headers.put(RDFGrpcProtocol.META_TXN_ID, session.txnId);
                        }
                        super.start(responseListener, headers);
                    }
                };
            }
        };
    }

    private <ReqT, RespT> void executeClientStream(
            Channel targetChannel,
            MethodDescriptor<ReqT, RespT> method,
            Consumer<StreamObserver<ReqT>> sender) {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> errorRef = new AtomicReference<>();
        ClientCall<ReqT, RespT> call = targetChannel.newCall(method, CallOptions.DEFAULT);
        StreamObserver<ReqT> requestObserver = ClientCalls.asyncClientStreamingCall(call, new StreamObserver<>() {
            @Override
            public void onNext(RespT value) {}

            @Override
            public void onError(Throwable t) {
                errorRef.set(t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        });
        try {
            sender.accept(requestObserver);
            requestObserver.onCompleted();
            latch.await();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            call.cancel("Interrupted", ex);
            throw new ARQException("Interrupted during gRPC stream", ex);
        } catch (RuntimeException ex) {
            requestObserver.onError(ex);
            throw ex;
        }
        Throwable err = errorRef.get();
        if ( err != null ) {
            if ( err instanceof StatusRuntimeException sre )
                throw RDFGrpcProtocol.fromStatusException(sre);
            throw new ARQException(err.getMessage(), err);
        }
    }

    /**
     * Client-side bidirectional transaction stream session. Binds the transaction lifecycle
     * to a single HTTP/2 stream so server-side write locks are released immediately if the
     * stream closes or is cancelled.
     */
    private static final class ClientTxnSession {
        private final ClientCall<TransactionRequest, TransactionResponse> call;
        private final StreamObserver<TransactionRequest> requestObserver;
        private final BlockingQueue<Object> responses = new LinkedBlockingQueue<>();
        private final TxnType txnType;
        private final Duration commandTimeout;
        private ReadWrite txnMode;
        private String txnId;
        private volatile boolean streamTerminated;
        private boolean closed;

        ClientTxnSession(Channel channel, TxnType txnType, Duration commandTimeout) {
            this.txnType = txnType;
            this.commandTimeout = commandTimeout;
            this.txnMode = TxnType.initial(txnType);
            this.call = channel.newCall(RDFGrpcProtocol.TRANSACTION_METHOD, CallOptions.DEFAULT);
            this.requestObserver = ClientCalls.asyncBidiStreamingCall(call, new StreamObserver<>() {
                @Override
                public void onNext(TransactionResponse value) {
                    responses.offer(value);
                }

                @Override
                public void onError(Throwable t) {
                    streamTerminated = true;
                    responses.offer(t);
                }

                @Override
                public void onCompleted() {
                    streamTerminated = true;
                    responses.offer(STREAM_EOF);
                }
            });
        }

        void begin() {
            try {
                TransactionResponse resp = sendCommand(TransactionRequest.newBuilder()
                    .setCommand(TransactionCommand.TRANSACTION_COMMAND_BEGIN)
                    .setTxnType(RDFGrpcProtocol.encodeTxnType(txnType))
                    .build());
                this.txnId = resp.getTxnId();
            } catch (RuntimeException ex) {
                cancelAbruptly();
                throw ex;
            }
        }

        /**
         * Send one command and wait for its response. The wait is unbounded unless a command
         * timeout was configured on the link. On timeout the stream is cancelled, which makes
         * the server abort the transaction unless the command was a {@code COMMIT} that it had
         * already carried out; the exception says so.
         */
        TransactionResponse sendCommand(TransactionRequest request) {
            if ( streamTerminated ) {
                throw new JenaTransactionException("Transaction stream is already closed");
            }
            requestObserver.onNext(request);
            try {
                Object event = (commandTimeout == null)
                    ? responses.take()
                    : responses.poll(commandTimeout.toNanos(), TimeUnit.NANOSECONDS);
                if ( event instanceof TransactionResponse resp ) {
                    return resp;
                }
                if ( event instanceof StatusRuntimeException sre ) {
                    throw RDFGrpcProtocol.fromStatusException(sre);
                }
                if ( event instanceof Throwable t ) {
                    throw new JenaTransactionException(t.getMessage(), t);
                }
                if ( event == null ) {
                    cancelAbruptly();
                    throw new JenaTransactionException(timeoutMessage(request.getCommand()));
                }
                throw new JenaTransactionException(
                    "Transaction stream closed before the " + request.getCommand() + " response arrived");
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                cancelAbruptly();
                throw new JenaTransactionException("Interrupted waiting for transaction response", ie);
            }
        }

        private String timeoutMessage(TransactionCommand command) {
            String base = "Timed out after " + commandTimeout + " waiting for the " + command + " response";
            if ( command == TransactionCommand.TRANSACTION_COMMAND_COMMIT ) {
                return base + "; the outcome of the commit is unknown";
            }
            return base + "; the transaction was cancelled";
        }

        void completeClientStream() {
            if ( closed )
                return;
            closed = true;
            try {
                requestObserver.onCompleted();
            } catch (RuntimeException ignored) {
                call.cancel("Closing transaction stream", null);
            }
        }

        void endAndClose() {
            if ( closed )
                return;
            closed = true;
            try {
                if ( !streamTerminated ) {
                    sendCommand(TransactionRequest.newBuilder()
                        .setCommand(TransactionCommand.TRANSACTION_COMMAND_END)
                        .build());
                }
                requestObserver.onCompleted();
            } catch (RuntimeException ignored) {
                call.cancel("Closing transaction stream", null);
            }
        }

        void cancelAbruptly() {
            if ( closed )
                return;
            closed = true;
            call.cancel("Abrupt transaction stream cancellation", null);
        }
    }
}
