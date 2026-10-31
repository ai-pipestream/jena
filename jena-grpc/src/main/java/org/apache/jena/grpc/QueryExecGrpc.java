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
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.protobuf.BoolValue;
import com.google.protobuf.StringValue;

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
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;
import org.apache.jena.riot.protobuf.ProtobufRDF;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_DataTuple;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_StreamRow;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_VarTuple;
import org.apache.jena.riot.system.StreamRDF;
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
 * using {@code PB_RDF} wire messages.
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
            ClientCall<StringValue, RDFSelectMessage> call = newTrackedCall(RDFGrpcProtocol.QUERY_SELECT_METHOD);
            Iterator<RDFSelectMessage> msgIter = ClientCalls.blockingServerStreamingCall(
                call, StringValue.of(queryString));
            RDF_VarTuple header = RDF_VarTuple.getDefaultInstance();
            if ( msgIter.hasNext() ) {
                RDFSelectMessage first = msgIter.next();
                if ( first.isHeader() ) {
                    header = first.getHeader();
                }
            }
            Iterator<RDF_DataTuple> rowIter = new Iterator<>() {
                @Override
                public boolean hasNext() {
                    if ( closed )
                        return false;
                    try {
                        return msgIter.hasNext();
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
                    try {
                        RDFSelectMessage msg = msgIter.next();
                        return msg.getRow();
                    } catch (StatusRuntimeException sre) {
                        throw RDFGrpcProtocol.fromStatusException(sre);
                    }
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
        return construct().find();
    }

    /** {@inheritDoc} */
    @Override
    public Iterator<Quad> constructQuads() {
        return constructDataset().find();
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
            ClientCall<StringValue, RDF_StreamRow> call = newTrackedCall(RDFGrpcProtocol.QUERY_DATASET_METHOD);
            Iterator<RDF_StreamRow> iter = ClientCalls.blockingServerStreamingCall(
                call, StringValue.of(queryString));
            StreamRDF dest = StreamRDFLib.dataset(dataset);
            Consumer<RDF_StreamRow> sink = ProtobufRDF.rowConsumerToStreamRDF(dest);
            dest.start();
            while ( iter.hasNext() ) {
                sink.accept(iter.next());
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
        return describe().find();
    }

    /** {@inheritDoc} */
    @Override
    public boolean ask() {
        try {
            ClientCall<StringValue, BoolValue> call = newTrackedCall(RDFGrpcProtocol.QUERY_ASK_METHOD);
            BoolValue resp = ClientCalls.blockingUnaryCall(
                call, StringValue.of(queryString));
            return resp.getValue();
        } catch (StatusRuntimeException sre) {
            throw RDFGrpcProtocol.fromStatusException(sre);
        }
    }

    /** {@inheritDoc} */
    @Override
    public JsonArray execJson() {
        try {
            ClientCall<StringValue, StringValue> call = newTrackedCall(RDFGrpcProtocol.QUERY_JSON_METHOD);
            StringValue resp = ClientCalls.blockingUnaryCall(
                call, StringValue.of(queryString));
            JsonValue val = JSON.parseAny(resp.getValue());
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

    private Graph executeGraphQuery(Graph target) {
        try {
            ClientCall<StringValue, RDF_StreamRow> call = newTrackedCall(RDFGrpcProtocol.QUERY_GRAPH_METHOD);
            Iterator<RDF_StreamRow> iter = ClientCalls.blockingServerStreamingCall(
                call, StringValue.of(queryString));
            StreamRDF dest = StreamRDFLib.graph(target);
            Consumer<RDF_StreamRow> sink = ProtobufRDF.rowConsumerToStreamRDF(dest);
            dest.start();
            while ( iter.hasNext() ) {
                sink.accept(iter.next());
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
