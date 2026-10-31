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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.protobuf.BoolValue;
import com.google.protobuf.Empty;
import com.google.protobuf.StringValue;

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
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_StreamRow;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Term;
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
import org.apache.jena.sparql.exec.QueryExecBuilder;
import org.apache.jena.sparql.exec.RowSet;
import org.apache.jena.sparql.exec.UpdateExecBuilder;
import org.apache.jena.sparql.graph.GraphFactory;
import org.apache.jena.system.Txn;
import org.apache.jena.update.Update;
import org.apache.jena.update.UpdateRequest;

/**
 * Jetty-free gRPC implementation of {@link RDFLink} communicating over {@code PB_RDF}
 * Protobuf messages, supporting streaming quad pattern matching ({@link #findStream}),
 * streaming dataset/graph ingest, transactional {@link RDFPatch} application and
 * subscription, and SPARQL Query/Update.
 */
public class RDFLinkGrpc implements RDFLink {

    private static final String ERR_CLOSED = "RDFLinkGrpc is closed";
    private static final String ERR_PATCH_NULL = "patch must not be null";
    private static final String ERR_PRODUCER_NULL = "producer must not be null";
    private static final String ERR_TARGET_NULL = "target must not be null";
    private static final String ERR_DEST_NULL = "dest must not be null";

    private final Channel rawChannel;
    private final Channel interceptedChannel;
    private final boolean ownChannel;
    private final boolean parseCheckQueries;
    private final boolean parseCheckUpdates;
    private final boolean encodeValues;
    private final int batchSize;
    private final ThreadLocal<String> currentTxnId = new ThreadLocal<>();
    private final ThreadLocal<TxnType> currentTxnType = new ThreadLocal<>();
    private final ThreadLocal<ReadWrite> currentTxnMode = new ThreadLocal<>();
    private volatile boolean open = true;

    RDFLinkGrpc(
            Channel channel,
            boolean ownChannel,
            boolean parseCheckQueries,
            boolean parseCheckUpdates,
            boolean encodeValues,
            int batchSize) {
        this.rawChannel = Objects.requireNonNull(channel);
        this.ownChannel = ownChannel;
        this.parseCheckQueries = parseCheckQueries;
        this.parseCheckUpdates = parseCheckUpdates;
        this.encodeValues = encodeValues;
        this.batchSize = batchSize;
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
     * Stream quads matching {@code (g, s, p, o)} from the remote dataset using {@code RDF_ANY}
     * wildcards for {@code null} or {@link Node#ANY} components.
     * <p>
     * This convenience method collects matching quads from the streaming RPC into an in-memory list
     * before returning the {@link Stream}. For non-buffering streaming consumption of large datasets,
     * use {@link #findStream(Node, Node, Node, Node, StreamRDF)} or
     * {@link #findStreamBatched(Node, Node, Node, Node, StreamRDF)}.
     *
     * @param g graph node pattern, or {@code null} / {@link Node#ANY} for any graph
     * @param s subject node pattern, or {@code null} / {@link Node#ANY} for any subject
     * @param p predicate node pattern, or {@code null} / {@link Node#ANY} for any predicate
     * @param o object node pattern, or {@code null} / {@link Node#ANY} for any object
     * @return a {@link Stream} of matching {@link Quad} instances
     */
    public Stream<Quad> findStream(Node g, Node s, Node p, Node o) {
        checkOpen();
        List<Quad> quads = new ArrayList<>();
        findStream(g, s, p, o, new StreamRDFBase() {
            @Override
            public void triple(Triple triple) {
                quads.add(Quad.create(Quad.defaultGraphIRI, triple));
            }

            @Override
            public void quad(Quad quad) {
                quads.add(quad);
            }
        });
        return StreamSupport.stream(
            Spliterators.spliterator(quads, Spliterator.ORDERED | Spliterator.NONNULL), false);
    }

    /**
     * Stream quads matching {@code (g, s, p, o)} from the remote dataset into {@code dest}
     * as individual {@link RDF_StreamRow} messages.
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
        try {
            Iterator<RDF_StreamRow> iter = ClientCalls.blockingServerStreamingCall(
                interceptedChannel, RDFGrpcProtocol.FIND_STREAM_METHOD, CallOptions.DEFAULT, pattern);
            Consumer<RDF_StreamRow> sink = ProtobufRDF.rowConsumerToStreamRDF(dest);
            dest.start();
            while ( iter.hasNext() ) {
                sink.accept(iter.next());
            }
            dest.finish();
        } catch (StatusRuntimeException sre) {
            throw RDFGrpcProtocol.fromStatusException(sre);
        }
    }

    /**
     * Stream quads matching {@code (g, s, p, o)} from the remote dataset into {@code dest}
     * using batched {@link RDF_Stream} messages.
     *
     * @param g graph node pattern, or {@code null} / {@link Node#ANY} for any graph
     * @param s subject node pattern, or {@code null} / {@link Node#ANY} for any subject
     * @param p predicate node pattern, or {@code null} / {@link Node#ANY} for any predicate
     * @param o object node pattern, or {@code null} / {@link Node#ANY} for any object
     * @param dest the destination {@link StreamRDF} sink
     * @throws IllegalArgumentException Thrown if {@code dest} is {@code null}
     */
    public void findStreamBatched(Node g, Node s, Node p, Node o, StreamRDF dest) {
        if ( dest == null )
            throw new IllegalArgumentException(ERR_DEST_NULL);
        checkOpen();
        RDF_Quad pattern = RDFGrpcProtocol.encodeQuadPattern(g, s, p, o);
        try {
            Iterator<RDF_Stream> iter = ClientCalls.blockingServerStreamingCall(
                interceptedChannel, RDFGrpcProtocol.FIND_STREAM_BATCHED_METHOD, CallOptions.DEFAULT, pattern);
            Consumer<RDF_Stream> sink = ProtobufRDF.batchConsumerToStreamRDF(dest);
            dest.start();
            while ( iter.hasNext() ) {
                sink.accept(iter.next());
            }
            dest.finish();
        } catch (StatusRuntimeException sre) {
            throw RDFGrpcProtocol.fromStatusException(sre);
        }
    }

    /**
     * Stream RDF triples and quads produced by {@code producer} into the remote dataset
     * as {@link RDF_StreamRow} messages without buffering the dataset in memory.
     *
     * @param producer callback that writes to the provided {@link StreamRDF}
     * @throws IllegalArgumentException Thrown if {@code producer} is {@code null}
     */
    public void streamLoadDataset(Consumer<StreamRDF> producer) {
        if ( producer == null )
            throw new IllegalArgumentException(ERR_PRODUCER_NULL);
        checkOpen();
        executeClientStream(interceptedChannel, RDFGrpcProtocol.LOAD_DATASET_METHOD, requestObserver -> {
            StreamRDF stream = ProtobufRDF.streamToRowConsumer(requestObserver::onNext, encodeValues);
            stream.start();
            producer.accept(stream);
            stream.finish();
        });
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
        executeClientStream(interceptedChannel, RDFGrpcProtocol.LOAD_DATASET_BATCHED_METHOD, requestObserver -> {
            StreamRDF stream = ProtobufRDF.streamToBatchConsumer(
                requestObserver::onNext, streamBatchSize, encodeValues);
            stream.start();
            producer.accept(stream);
            stream.finish();
        });
    }

    /**
     * Apply an {@link RDFPatch} to the remote dataset by streaming {@link RDF_Patch_Row} messages.
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
     * {@link RDF_Patch_Row} messages.
     *
     * @param producer callback that emits patch changes
     * @throws IllegalArgumentException Thrown if {@code producer} is {@code null}
     */
    public void applyPatch(Consumer<RDFChanges> producer) {
        if ( producer == null )
            throw new IllegalArgumentException(ERR_PRODUCER_NULL);
        checkOpen();
        executeClientStream(interceptedChannel, RDFGrpcProtocol.APPLY_PATCH_METHOD, requestObserver -> {
            RDFChanges writer = RDFPatchOps.protobufWriter(requestObserver::onNext);
            writer.start();
            try {
                producer.accept(writer);
            } finally {
                writer.finish();
            }
        });
    }

    /**
     * Apply an {@link RDFPatch} to the remote dataset as a single batched {@link RDF_Patch} message.
     *
     * @param patch the patch to apply
     * @throws IllegalArgumentException Thrown if {@code patch} is {@code null}
     */
    public void applyPatchBatch(RDFPatch patch) {
        if ( patch == null )
            throw new IllegalArgumentException(ERR_PATCH_NULL);
        checkOpen();
        RDF_Patch batch = RDFChangesWriterProtobuf.toProtobuf(patch);
        try {
            ClientCalls.blockingUnaryCall(
                interceptedChannel, RDFGrpcProtocol.APPLY_PATCH_BATCH_METHOD, CallOptions.DEFAULT, batch);
        } catch (StatusRuntimeException sre) {
            throw RDFGrpcProtocol.fromStatusException(sre);
        }
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
        if ( target == null )
            throw new IllegalArgumentException(ERR_TARGET_NULL);
        checkOpen();
        ClientCall<Empty, RDF_Patch_Row> call =
            interceptedChannel.newCall(RDFGrpcProtocol.SUBSCRIBE_PATCHES_METHOD, CallOptions.DEFAULT);
        Consumer<RDF_Patch_Row> sink = RDFPatchOps.protobufChangesReceiver(target);
        CountDownLatch readyLatch = new CountDownLatch(1);
        AtomicReference<Throwable> errorRef = new AtomicReference<>();
        ClientCalls.asyncServerStreamingCall(call, Empty.getDefaultInstance(), new StreamObserver<>() {
            @Override
            public void onNext(RDF_Patch_Row value) {
                if ( value.getRowCase() == RDF_Patch_Row.RowCase.ROW_NOT_SET ) {
                    readyLatch.countDown();
                    return;
                }
                readyLatch.countDown();
                sink.accept(value);
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
        RDF_Term term = RDFGrpcProtocol.encodeGraphNode(graphName);
        try {
            Iterator<RDF_StreamRow> iter = ClientCalls.blockingServerStreamingCall(
                interceptedChannel, RDFGrpcProtocol.GET_GRAPH_METHOD, CallOptions.DEFAULT, term);
            Graph graph = GraphFactory.createDefaultGraph();
            StreamRDF dest = StreamRDFLib.graph(graph);
            Consumer<RDF_StreamRow> sink = ProtobufRDF.rowConsumerToStreamRDF(dest);
            dest.start();
            while ( iter.hasNext() ) {
                sink.accept(iter.next());
            }
            dest.finish();
            return graph;
        } catch (StatusRuntimeException sre) {
            throw RDFGrpcProtocol.fromStatusException(sre);
        }
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
        Channel graphChannel = withGraphHeader(graphName);
        executeClientStream(graphChannel, RDFGrpcProtocol.LOAD_GRAPH_METHOD, requestObserver -> {
            StreamRDF encoder = ProtobufRDF.streamToRowConsumer(requestObserver::onNext, encodeValues);
            encoder.start();
            StreamRDFOps.sendGraphToStream(graph, encoder);
            encoder.finish();
        });
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
        Channel graphChannel = withGraphHeader(graphName);
        executeClientStream(graphChannel, RDFGrpcProtocol.PUT_GRAPH_METHOD, requestObserver -> {
            StreamRDF encoder = ProtobufRDF.streamToRowConsumer(requestObserver::onNext, encodeValues);
            encoder.start();
            StreamRDFOps.sendGraphToStream(graph, encoder);
            encoder.finish();
        });
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
        RDF_Term term = RDFGrpcProtocol.encodeGraphNode(graphName);
        try {
            ClientCalls.blockingUnaryCall(
                interceptedChannel, RDFGrpcProtocol.DELETE_GRAPH_METHOD, CallOptions.DEFAULT, term);
        } catch (StatusRuntimeException sre) {
            throw RDFGrpcProtocol.fromStatusException(sre);
        }
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
        try {
            Iterator<RDF_StreamRow> iter = ClientCalls.blockingServerStreamingCall(
                interceptedChannel, RDFGrpcProtocol.GET_DATASET_METHOD, CallOptions.DEFAULT, Empty.getDefaultInstance());
            DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
            StreamRDF dest = StreamRDFLib.dataset(dsg);
            Consumer<RDF_StreamRow> sink = ProtobufRDF.rowConsumerToStreamRDF(dest);
            dest.start();
            while ( iter.hasNext() ) {
                sink.accept(iter.next());
            }
            dest.finish();
            return dsg;
        } catch (StatusRuntimeException sre) {
            throw RDFGrpcProtocol.fromStatusException(sre);
        }
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
        executeClientStream(interceptedChannel, RDFGrpcProtocol.PUT_DATASET_METHOD, requestObserver -> {
            StreamRDF encoder = ProtobufRDF.streamToRowConsumer(requestObserver::onNext, encodeValues);
            RDFDataMgr.parse(encoder, file);
        });
    }

    /** {@inheritDoc} */
    @Override
    public void putDataset(DatasetGraph dataset) {
        Objects.requireNonNull(dataset);
        checkOpen();
        executeClientStream(interceptedChannel, RDFGrpcProtocol.PUT_DATASET_METHOD, requestObserver -> {
            StreamRDF encoder = ProtobufRDF.streamToRowConsumer(requestObserver::onNext, encodeValues);
            encoder.start();
            StreamRDFOps.sendDatasetToStream(dataset, encoder);
            encoder.finish();
        });
    }

    /** {@inheritDoc} */
    @Override
    public void clearDataset() {
        checkOpen();
        try {
            ClientCalls.blockingUnaryCall(
                interceptedChannel, RDFGrpcProtocol.CLEAR_DATASET_METHOD, CallOptions.DEFAULT, Empty.getDefaultInstance());
        } catch (StatusRuntimeException sre) {
            throw RDFGrpcProtocol.fromStatusException(sre);
        }
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
        try {
            StringValue resp = ClientCalls.blockingUnaryCall(
                rawChannel, RDFGrpcProtocol.BEGIN_TXN_METHOD, CallOptions.DEFAULT, StringValue.of(txnType.name()));
            currentTxnId.set(resp.getValue());
            currentTxnType.set(txnType);
            currentTxnMode.set(TxnType.initial(txnType));
        } catch (StatusRuntimeException sre) {
            throw RDFGrpcProtocol.fromStatusException(sre);
        }
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
        String txnId = currentTxnId.get();
        if ( txnId == null ) {
            throw new JenaTransactionException("Not in a transaction");
        }
        if ( currentTxnMode.get() == ReadWrite.WRITE ) {
            return true;
        }
        TxnType txnType = currentTxnType.get();
        if ( txnType == TxnType.READ ) {
            return false;
        }
        Promote mode = (promote != null) ? promote : Promote.ISOLATED;
        try {
            BoolValue resp = ClientCalls.blockingUnaryCall(
                rawChannel,
                RDFGrpcProtocol.PROMOTE_TXN_METHOD,
                CallOptions.DEFAULT,
                StringValue.of(txnId + ":" + mode.name()));
            if ( resp.getValue() ) {
                currentTxnMode.set(ReadWrite.WRITE);
                return true;
            }
            return false;
        } catch (StatusRuntimeException sre) {
            throw RDFGrpcProtocol.fromStatusException(sre);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void commit() {
        checkOpen();
        String txnId = currentTxnId.get();
        if ( txnId == null ) {
            throw new JenaTransactionException("Not in a transaction");
        }
        try {
            ClientCalls.blockingUnaryCall(
                rawChannel, RDFGrpcProtocol.COMMIT_TXN_METHOD, CallOptions.DEFAULT, StringValue.of(txnId));
        } catch (StatusRuntimeException sre) {
            throw RDFGrpcProtocol.fromStatusException(sre);
        } finally {
            // Always release the server session and local state, even if the commit call failed.
            clearLocalTxnState();
            endRemoteTxn(txnId);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void abort() {
        checkOpen();
        String txnId = currentTxnId.get();
        if ( txnId == null ) {
            throw new JenaTransactionException("Not in a transaction");
        }
        try {
            ClientCalls.blockingUnaryCall(
                rawChannel, RDFGrpcProtocol.ABORT_TXN_METHOD, CallOptions.DEFAULT, StringValue.of(txnId));
        } catch (StatusRuntimeException sre) {
            throw RDFGrpcProtocol.fromStatusException(sre);
        } finally {
            // Always release the server session and local state, even if the abort call failed.
            clearLocalTxnState();
            endRemoteTxn(txnId);
        }
    }

    private void endRemoteTxn(String txnId) {
        try {
            ClientCalls.blockingUnaryCall(
                rawChannel, RDFGrpcProtocol.END_TXN_METHOD, CallOptions.DEFAULT, StringValue.of(txnId));
        } catch (StatusRuntimeException ignored) {
            // Best effort: the server also ends idle transactions after a timeout
        }
    }

    private void clearLocalTxnState() {
        currentTxnId.remove();
        currentTxnType.remove();
        currentTxnMode.remove();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isInTransaction() {
        return currentTxnId.get() != null;
    }

    /** {@inheritDoc} */
    @Override
    public void end() {
        String txnId = currentTxnId.get();
        ReadWrite mode = currentTxnMode.get();
        clearLocalTxnState();
        if ( txnId != null ) {
            endRemoteTxn(txnId);
            if ( mode == ReadWrite.WRITE ) {
                throw new JenaTransactionException("Write transaction - no commit or abort before end()");
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public ReadWrite transactionMode() {
        return currentTxnMode.get();
    }

    /** {@inheritDoc} */
    @Override
    public TxnType transactionType() {
        return currentTxnType.get();
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
        String txnId = currentTxnId.get();
        clearLocalTxnState();
        if ( txnId != null ) {
            try {
                ClientCalls.blockingUnaryCall(
                    rawChannel, RDFGrpcProtocol.END_TXN_METHOD, CallOptions.DEFAULT, StringValue.of(txnId));
            } catch (RuntimeException ignored) {
                // Ignore during close
            }
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
                        String txnId = currentTxnId.get();
                        if ( txnId != null ) {
                            headers.put(RDFGrpcProtocol.META_TXN_ID, txnId);
                        }
                        super.start(responseListener, headers);
                    }
                };
            }
        };
    }

    private Channel withGraphHeader(Node graphName) {
        RDF_Term term = RDFGrpcProtocol.encodeGraphNode(graphName);
        byte[] termBytes = term.toByteArray();
        ClientInterceptor graphInterceptor = new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                    MethodDescriptor<ReqT, RespT> method,
                    CallOptions callOptions,
                    Channel next) {
                return new SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        headers.put(RDFGrpcProtocol.META_GRAPH_TERM_BIN, termBytes);
                        super.start(responseListener, headers);
                    }
                };
            }
        };
        return ClientInterceptors.intercept(interceptedChannel, graphInterceptor);
    }

    private <ReqT> void executeClientStream(
            Channel targetChannel,
            MethodDescriptor<ReqT, Empty> method,
            Consumer<StreamObserver<ReqT>> sender) {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> errorRef = new AtomicReference<>();
        ClientCall<ReqT, Empty> call = targetChannel.newCall(method, CallOptions.DEFAULT);
        StreamObserver<ReqT> requestObserver = ClientCalls.asyncClientStreamingCall(call, new StreamObserver<>() {
            @Override
            public void onNext(Empty value) {}

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
}
