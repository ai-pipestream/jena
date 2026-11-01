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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCalls;
import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonArray;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.json.JsonValue;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.grpc.wire.PB_RDFService.ExecuteQueryRequest;
import org.apache.jena.grpc.wire.PB_RDFService.ExecuteQueryResponse;
import org.apache.jena.grpc.wire.PB_RDFService.QueryResultForm;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;
import org.apache.jena.riot.protobuf.ProtobufRDF;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_DataTuple;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Stream;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_VarTuple;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFBase;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.exec.QueryExec;
import org.apache.jena.sparql.exec.RowSet;
import org.apache.jena.sparql.graph.GraphFactory;
import org.apache.jena.sparql.util.Context;

/**
 * {@link QueryExec} implementation executing SPARQL queries over a gRPC {@link Channel}
 * using the stream-first {@code ExecuteQuery} RPC.
 */
public class QueryExecGrpc implements QueryExec {

    private final Channel channel;
    private final Supplier<CallOptions> callOptionsSupplier;
    private Query query;
    private final String queryString;
    private final Context context;
    private final long timeout;
    private final TimeUnit timeoutUnit;
    private volatile ClientCall<?, ?> activeCall;
    private volatile boolean closed;

    QueryExecGrpc(
            Channel channel,
            Supplier<CallOptions> callOptionsSupplier,
            Query query,
            String queryString,
            Context context,
            long timeout,
            TimeUnit timeoutUnit) {
        this.channel = channel;
        this.callOptionsSupplier = callOptionsSupplier;
        this.query = query;
        this.queryString = queryString;
        this.context = context;
        this.timeout = timeout;
        this.timeoutUnit = timeoutUnit;
    }

    /** {@inheritDoc} */
    @Override
    public DatasetGraph getDataset() {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Context getContext() {
        return context;
    }

    /** {@inheritDoc} */
    @Override
    public Query getQuery() {
        if ( query == null && queryString != null ) {
            try {
                query = QueryFactory.create(queryString, Syntax.syntaxARQ);
            } catch (Exception ignored) {
                return null;
            }
        }
        return query;
    }

    /** {@inheritDoc} */
    @Override
    public String getQueryString() {
        return queryString;
    }

    /** {@inheritDoc} */
    @Override
    public RowSet select() {
        try {
            ClientCall<ExecuteQueryRequest, ExecuteQueryResponse> call =
                newTrackedCall(RDFGrpcProtocol.EXECUTE_QUERY_METHOD);
            Iterator<ExecuteQueryResponse> msgIter = ClientCalls.blockingServerStreamingCall(
                call, buildQueryRequest(QueryResultForm.QUERY_RESULT_FORM_SELECT));
            RDF_VarTuple header = RDF_VarTuple.getDefaultInstance();
            Iterator<RDF_DataTuple> firstBatchIter = Collections.emptyIterator();
            if ( msgIter.hasNext() ) {
                ExecuteQueryResponse first = msgIter.next();
                if ( first.getResultCase() == ExecuteQueryResponse.ResultCase.SELECT_HEADER ) {
                    header = first.getSelectHeader();
                } else if ( first.getResultCase() == ExecuteQueryResponse.ResultCase.SELECT_ROWS ) {
                    firstBatchIter = first.getSelectRows().getRowsList().iterator();
                }
            }
            Iterator<RDF_DataTuple> initialBatch = firstBatchIter;
            Iterator<RDF_DataTuple> rowIter = new Iterator<>() {
                private Iterator<RDF_DataTuple> currentBatch = initialBatch;

                @Override
                public boolean hasNext() {
                    if ( closed )
                        return false;
                    try {
                        while ( !currentBatch.hasNext() ) {
                            if ( !msgIter.hasNext() )
                                return false;
                            ExecuteQueryResponse nextMsg = msgIter.next();
                            if ( nextMsg.getResultCase() == ExecuteQueryResponse.ResultCase.SELECT_ROWS ) {
                                currentBatch = nextMsg.getSelectRows().getRowsList().iterator();
                            }
                        }
                        return true;
                    } catch (StatusRuntimeException sre) {
                        if ( closed )
                            return false;
                        throw RDFGrpcProtocol.fromStatusException(sre);
                    }
                }

                @Override
                public RDF_DataTuple next() {
                    if ( !hasNext() )
                        throw new NoSuchElementException();
                    return currentBatch.next();
                }
            };
            return ProtobufRDF.readRowSet(header, rowIter);
        } catch (StatusRuntimeException sre) {
            throw RDFGrpcProtocol.fromStatusException(sre);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Graph construct() {
        return construct(GraphFactory.createDefaultGraph());
    }

    /** {@inheritDoc} */
    @Override
    public Graph construct(Graph graph) {
        return executeGraphQuery(graph);
    }

    /** {@inheritDoc} */
    @Override
    public Iterator<Triple> constructTriples() {
        return streamGraphTriples();
    }

    /** {@inheritDoc} */
    @Override
    public Iterator<Quad> constructQuads() {
        try {
            ClientCall<ExecuteQueryRequest, ExecuteQueryResponse> call =
                newTrackedCall(RDFGrpcProtocol.EXECUTE_QUERY_METHOD);
            Iterator<ExecuteQueryResponse> msgIter = ClientCalls.blockingServerStreamingCall(
                call, buildQueryRequest(QueryResultForm.QUERY_RESULT_FORM_DATASET));
            ArrayDeque<Quad> buffer = new ArrayDeque<>();
            Consumer<RDF_Stream> sink = ProtobufRDF.batchConsumerToStreamRDF(new StreamRDFBase() {
                @Override
                public void triple(Triple triple) {
                    buffer.addLast(Quad.create(Quad.defaultGraphIRI, triple));
                }

                @Override
                public void quad(Quad quad) {
                    buffer.addLast(quad);
                }
            });
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    if ( closed )
                        return false;
                    try {
                        while ( buffer.isEmpty() && msgIter.hasNext() ) {
                            ExecuteQueryResponse resp = msgIter.next();
                            if ( resp.getResultCase() == ExecuteQueryResponse.ResultCase.RDF_BATCH ) {
                                sink.accept(resp.getRdfBatch());
                            }
                        }
                        return !buffer.isEmpty();
                    } catch (StatusRuntimeException sre) {
                        if ( closed )
                            return false;
                        throw RDFGrpcProtocol.fromStatusException(sre);
                    }
                }

                @Override
                public Quad next() {
                    if ( !hasNext() )
                        throw new NoSuchElementException();
                    return buffer.removeFirst();
                }
            };
        } catch (StatusRuntimeException sre) {
            throw RDFGrpcProtocol.fromStatusException(sre);
        }
    }

    /** {@inheritDoc} */
    @Override
    public DatasetGraph constructDataset() {
        return constructDataset(DatasetGraphFactory.createGeneral());
    }

    /** {@inheritDoc} */
    @Override
    public DatasetGraph constructDataset(DatasetGraph dataset) {
        try {
            ClientCall<ExecuteQueryRequest, ExecuteQueryResponse> call =
                newTrackedCall(RDFGrpcProtocol.EXECUTE_QUERY_METHOD);
            Iterator<ExecuteQueryResponse> iter = ClientCalls.blockingServerStreamingCall(
                call, buildQueryRequest(QueryResultForm.QUERY_RESULT_FORM_DATASET));
            StreamRDF dest = StreamRDFLib.dataset(dataset);
            Consumer<RDF_Stream> sink = ProtobufRDF.batchConsumerToStreamRDF(dest);
            dest.start();
            while ( iter.hasNext() ) {
                ExecuteQueryResponse resp = iter.next();
                if ( resp.getResultCase() == ExecuteQueryResponse.ResultCase.RDF_BATCH ) {
                    sink.accept(resp.getRdfBatch());
                }
            }
            dest.finish();
            return dataset;
        } catch (StatusRuntimeException sre) {
            throw RDFGrpcProtocol.fromStatusException(sre);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Graph describe() {
        return describe(GraphFactory.createDefaultGraph());
    }

    /** {@inheritDoc} */
    @Override
    public Graph describe(Graph graph) {
        return executeGraphQuery(graph);
    }

    /** {@inheritDoc} */
    @Override
    public Iterator<Triple> describeTriples() {
        return streamGraphTriples();
    }

    /** {@inheritDoc} */
    @Override
    public boolean ask() {
        try {
            ClientCall<ExecuteQueryRequest, ExecuteQueryResponse> call =
                newTrackedCall(RDFGrpcProtocol.EXECUTE_QUERY_METHOD);
            Iterator<ExecuteQueryResponse> iter = ClientCalls.blockingServerStreamingCall(
                call, buildQueryRequest(QueryResultForm.QUERY_RESULT_FORM_ASK));
            if ( iter.hasNext() ) {
                return iter.next().getAskResult();
            }
            return false;
        } catch (StatusRuntimeException sre) {
            throw RDFGrpcProtocol.fromStatusException(sre);
        }
    }

    /** {@inheritDoc} */
    @Override
    public JsonArray execJson() {
        try {
            ClientCall<ExecuteQueryRequest, ExecuteQueryResponse> call =
                newTrackedCall(RDFGrpcProtocol.EXECUTE_QUERY_METHOD);
            Iterator<ExecuteQueryResponse> iter = ClientCalls.blockingServerStreamingCall(
                call, buildQueryRequest(QueryResultForm.QUERY_RESULT_FORM_JSON));
            String jsonStr = iter.hasNext() ? iter.next().getJsonResult() : "[]";
            JsonValue val = JSON.parseAny(jsonStr);
            return val.getAsArray();
        } catch (StatusRuntimeException sre) {
            throw RDFGrpcProtocol.fromStatusException(sre);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Iterator<JsonObject> execJsonItems() {
        JsonArray array = execJson();
        List<JsonObject> items = new ArrayList<>(array.size());
        for ( JsonValue item : array ) {
            items.add(item.getAsObject());
        }
        return items.iterator();
    }

    /** {@inheritDoc} */
    @Override
    public void abort() {
        closed = true;
        ClientCall<?, ?> call = activeCall;
        if ( call != null ) {
            call.cancel("QueryExec aborted", null);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        closed = true;
        ClientCall<?, ?> call = activeCall;
        if ( call != null ) {
            call.cancel("QueryExec closed", null);
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean isClosed() {
        return closed;
    }

    private ExecuteQueryRequest buildQueryRequest(QueryResultForm form) {
        return ExecuteQueryRequest.newBuilder()
            .setQuery(queryString)
            .setResultForm(form)
            .build();
    }

    private Iterator<Triple> streamGraphTriples() {
        try {
            ClientCall<ExecuteQueryRequest, ExecuteQueryResponse> call =
                newTrackedCall(RDFGrpcProtocol.EXECUTE_QUERY_METHOD);
            Iterator<ExecuteQueryResponse> msgIter = ClientCalls.blockingServerStreamingCall(
                call, buildQueryRequest(QueryResultForm.QUERY_RESULT_FORM_GRAPH));
            ArrayDeque<Triple> buffer = new ArrayDeque<>();
            Consumer<RDF_Stream> sink = ProtobufRDF.batchConsumerToStreamRDF(new StreamRDFBase() {
                @Override
                public void triple(Triple triple) {
                    buffer.addLast(triple);
                }

                @Override
                public void quad(Quad quad) {
                    buffer.addLast(quad.asTriple());
                }
            });
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    if ( closed )
                        return false;
                    try {
                        while ( buffer.isEmpty() && msgIter.hasNext() ) {
                            ExecuteQueryResponse resp = msgIter.next();
                            if ( resp.getResultCase() == ExecuteQueryResponse.ResultCase.RDF_BATCH ) {
                                sink.accept(resp.getRdfBatch());
                            }
                        }
                        return !buffer.isEmpty();
                    } catch (StatusRuntimeException sre) {
                        if ( closed )
                            return false;
                        throw RDFGrpcProtocol.fromStatusException(sre);
                    }
                }

                @Override
                public Triple next() {
                    if ( !hasNext() )
                        throw new NoSuchElementException();
                    return buffer.removeFirst();
                }
            };
        } catch (StatusRuntimeException sre) {
            throw RDFGrpcProtocol.fromStatusException(sre);
        }
    }

    private Graph executeGraphQuery(Graph target) {
        try {
            ClientCall<ExecuteQueryRequest, ExecuteQueryResponse> call =
                newTrackedCall(RDFGrpcProtocol.EXECUTE_QUERY_METHOD);
            Iterator<ExecuteQueryResponse> iter = ClientCalls.blockingServerStreamingCall(
                call, buildQueryRequest(QueryResultForm.QUERY_RESULT_FORM_GRAPH));
            StreamRDF dest = StreamRDFLib.graph(target);
            Consumer<RDF_Stream> sink = ProtobufRDF.batchConsumerToStreamRDF(dest);
            dest.start();
            while ( iter.hasNext() ) {
                ExecuteQueryResponse resp = iter.next();
                if ( resp.getResultCase() == ExecuteQueryResponse.ResultCase.RDF_BATCH ) {
                    sink.accept(resp.getRdfBatch());
                }
            }
            dest.finish();
            return target;
        } catch (StatusRuntimeException sre) {
            throw RDFGrpcProtocol.fromStatusException(sre);
        }
    }

    private <ReqT, RespT> ClientCall<ReqT, RespT> newTrackedCall(MethodDescriptor<ReqT, RespT> method) {
        ClientCall<ReqT, RespT> call = channel.newCall(method, effectiveCallOptions());
        activeCall = call;
        if ( closed ) {
            call.cancel("QueryExec closed", null);
        }
        return call;
    }

    private CallOptions effectiveCallOptions() {
        CallOptions opts = callOptionsSupplier.get();
        if ( timeout > 0 && timeoutUnit != null ) {
            opts = opts.withDeadlineAfter(timeout, timeoutUnit);
        }
        return opts;
    }
}
