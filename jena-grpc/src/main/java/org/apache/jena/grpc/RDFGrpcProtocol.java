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

import com.google.protobuf.BoolValue;
import com.google.protobuf.Empty;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;

import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.ProtoUtils;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.graph.Node;
import org.apache.jena.query.QueryException;
import org.apache.jena.query.QueryParseException;
import org.apache.jena.riot.protobuf.ProtobufConvert;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Patch;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Patch_Row;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Quad;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Stream;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_StreamRow;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Term;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_UNDEF;
import org.apache.jena.sparql.ARQException;
import org.apache.jena.sparql.JenaTransactionException;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.update.UpdateException;

/**
 * gRPC service descriptors, metadata keys, and wire helpers for the Apache Jena
 * {@code RDFDatasetService} protocol over {@code PB_RDF} messages.
 */
public final class RDFGrpcProtocol {

    /** Full gRPC service name for {@link RDFDatasetGrpcService}. */
    public static final String SERVICE_NAME = "org.apache.jena.grpc.RDFDatasetService";

    /** Prefix for query parse errors propagated in gRPC status descriptions. */
    static final String PREFIX_QUERY_PARSE_ERROR = "QUERY_PARSE_ERROR:";
    /** Prefix for query execution errors propagated in gRPC status descriptions. */
    static final String PREFIX_QUERY_ERROR = "QUERY_ERROR:";
    /** Prefix for update execution errors propagated in gRPC status descriptions. */
    static final String PREFIX_UPDATE_ERROR = "UPDATE_ERROR:";
    /** Prefix for transaction errors propagated in gRPC status descriptions. */
    static final String PREFIX_TXN_ERROR = "TXN_ERROR:";

    /** Metadata header key carrying an active remote transaction identifier. */
    public static final Metadata.Key<String> META_TXN_ID =
        Metadata.Key.of("x-jena-txn-id", Metadata.ASCII_STRING_MARSHALLER);

    /** Binary metadata header key carrying the target graph {@link RDF_Term} for graph ingest. */
    public static final Metadata.Key<byte[]> META_GRAPH_TERM_BIN =
        Metadata.Key.of("x-jena-graph-term-bin", Metadata.BINARY_BYTE_MARSHALLER);

    /** Server context key holding the active transaction identifier from request metadata. */
    static final Context.Key<String> CTX_TXN_ID = Context.key("jena-txn-id");

    /** Server context key holding the binary target graph {@link RDF_Term} from request metadata. */
    static final Context.Key<byte[]> CTX_GRAPH_TERM_BIN = Context.key("jena-graph-term-bin");

    private static final RDF_Term UNDEF_TERM =
        RDF_Term.newBuilder().setUndefined(RDF_UNDEF.getDefaultInstance()).build();

    /** Streaming quad pattern match returning {@link RDF_StreamRow} messages. */
    public static final MethodDescriptor<RDF_Quad, RDF_StreamRow> FIND_STREAM_METHOD =
        MethodDescriptor.<RDF_Quad, RDF_StreamRow>newBuilder()
            .setType(MethodType.SERVER_STREAMING)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "FindStream"))
            .setSafe(true)
            .setRequestMarshaller(ProtoUtils.marshaller(RDF_Quad.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(RDF_StreamRow.getDefaultInstance()))
            .build();

    /** Streaming quad pattern match returning batched {@link RDF_Stream} messages. */
    public static final MethodDescriptor<RDF_Quad, RDF_Stream> FIND_STREAM_BATCHED_METHOD =
        MethodDescriptor.<RDF_Quad, RDF_Stream>newBuilder()
            .setType(MethodType.SERVER_STREAMING)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "FindStreamBatched"))
            .setSafe(true)
            .setRequestMarshaller(ProtoUtils.marshaller(RDF_Quad.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(RDF_Stream.getDefaultInstance()))
            .build();

    /** Fetch a graph (default or named) as a stream of {@link RDF_StreamRow} messages. */
    public static final MethodDescriptor<RDF_Term, RDF_StreamRow> GET_GRAPH_METHOD =
        MethodDescriptor.<RDF_Term, RDF_StreamRow>newBuilder()
            .setType(MethodType.SERVER_STREAMING)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "GetGraph"))
            .setSafe(true)
            .setRequestMarshaller(ProtoUtils.marshaller(RDF_Term.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(RDF_StreamRow.getDefaultInstance()))
            .build();

    /** Append a client-streamed sequence of {@link RDF_StreamRow} messages into a graph. */
    public static final MethodDescriptor<RDF_StreamRow, Empty> LOAD_GRAPH_METHOD =
        MethodDescriptor.<RDF_StreamRow, Empty>newBuilder()
            .setType(MethodType.CLIENT_STREAMING)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "LoadGraph"))
            .setRequestMarshaller(ProtoUtils.marshaller(RDF_StreamRow.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(Empty.getDefaultInstance()))
            .build();

    /** Replace a graph with a client-streamed sequence of {@link RDF_StreamRow} messages. */
    public static final MethodDescriptor<RDF_StreamRow, Empty> PUT_GRAPH_METHOD =
        MethodDescriptor.<RDF_StreamRow, Empty>newBuilder()
            .setType(MethodType.CLIENT_STREAMING)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "PutGraph"))
            .setRequestMarshaller(ProtoUtils.marshaller(RDF_StreamRow.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(Empty.getDefaultInstance()))
            .build();

    /** Delete or clear a graph identified by {@link RDF_Term}. */
    public static final MethodDescriptor<RDF_Term, Empty> DELETE_GRAPH_METHOD =
        MethodDescriptor.<RDF_Term, Empty>newBuilder()
            .setType(MethodType.UNARY)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "DeleteGraph"))
            .setRequestMarshaller(ProtoUtils.marshaller(RDF_Term.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(Empty.getDefaultInstance()))
            .build();

    /** Fetch the entire dataset as a stream of {@link RDF_StreamRow} messages. */
    public static final MethodDescriptor<Empty, RDF_StreamRow> GET_DATASET_METHOD =
        MethodDescriptor.<Empty, RDF_StreamRow>newBuilder()
            .setType(MethodType.SERVER_STREAMING)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "GetDataset"))
            .setSafe(true)
            .setRequestMarshaller(ProtoUtils.marshaller(Empty.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(RDF_StreamRow.getDefaultInstance()))
            .build();

    /** Append a client-streamed sequence of {@link RDF_StreamRow} messages into the dataset. */
    public static final MethodDescriptor<RDF_StreamRow, Empty> LOAD_DATASET_METHOD =
        MethodDescriptor.<RDF_StreamRow, Empty>newBuilder()
            .setType(MethodType.CLIENT_STREAMING)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "LoadDataset"))
            .setRequestMarshaller(ProtoUtils.marshaller(RDF_StreamRow.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(Empty.getDefaultInstance()))
            .build();

    /** Append a client-streamed sequence of batched {@link RDF_Stream} messages into the dataset. */
    public static final MethodDescriptor<RDF_Stream, Empty> LOAD_DATASET_BATCHED_METHOD =
        MethodDescriptor.<RDF_Stream, Empty>newBuilder()
            .setType(MethodType.CLIENT_STREAMING)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "LoadDatasetBatched"))
            .setRequestMarshaller(ProtoUtils.marshaller(RDF_Stream.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(Empty.getDefaultInstance()))
            .build();

    /** Replace the dataset contents with a client-streamed sequence of {@link RDF_StreamRow} messages. */
    public static final MethodDescriptor<RDF_StreamRow, Empty> PUT_DATASET_METHOD =
        MethodDescriptor.<RDF_StreamRow, Empty>newBuilder()
            .setType(MethodType.CLIENT_STREAMING)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "PutDataset"))
            .setRequestMarshaller(ProtoUtils.marshaller(RDF_StreamRow.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(Empty.getDefaultInstance()))
            .build();

    /** Clear all default and named graphs in the dataset. */
    public static final MethodDescriptor<Empty, Empty> CLEAR_DATASET_METHOD =
        MethodDescriptor.<Empty, Empty>newBuilder()
            .setType(MethodType.UNARY)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "ClearDataset"))
            .setRequestMarshaller(ProtoUtils.marshaller(Empty.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(Empty.getDefaultInstance()))
            .build();

    /** Execute a SPARQL {@code SELECT} query and stream {@link RDFSelectMessage} frames. */
    public static final MethodDescriptor<StringValue, RDFSelectMessage> QUERY_SELECT_METHOD =
        MethodDescriptor.<StringValue, RDFSelectMessage>newBuilder()
            .setType(MethodType.SERVER_STREAMING)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "QuerySelect"))
            .setSafe(true)
            .setRequestMarshaller(ProtoUtils.marshaller(StringValue.getDefaultInstance()))
            .setResponseMarshaller(RDFSelectMessage.marshaller())
            .build();

    /** Execute a SPARQL {@code CONSTRUCT} or {@code DESCRIBE} graph query and stream {@link RDF_StreamRow} messages. */
    public static final MethodDescriptor<StringValue, RDF_StreamRow> QUERY_GRAPH_METHOD =
        MethodDescriptor.<StringValue, RDF_StreamRow>newBuilder()
            .setType(MethodType.SERVER_STREAMING)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "QueryGraph"))
            .setSafe(true)
            .setRequestMarshaller(ProtoUtils.marshaller(StringValue.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(RDF_StreamRow.getDefaultInstance()))
            .build();

    /** Execute a SPARQL {@code CONSTRUCT} dataset query and stream {@link RDF_StreamRow} messages. */
    public static final MethodDescriptor<StringValue, RDF_StreamRow> QUERY_DATASET_METHOD =
        MethodDescriptor.<StringValue, RDF_StreamRow>newBuilder()
            .setType(MethodType.SERVER_STREAMING)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "QueryDataset"))
            .setSafe(true)
            .setRequestMarshaller(ProtoUtils.marshaller(StringValue.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(RDF_StreamRow.getDefaultInstance()))
            .build();

    /** Execute a SPARQL {@code ASK} query and return a {@link BoolValue}. */
    public static final MethodDescriptor<StringValue, BoolValue> QUERY_ASK_METHOD =
        MethodDescriptor.<StringValue, BoolValue>newBuilder()
            .setType(MethodType.UNARY)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "QueryAsk"))
            .setSafe(true)
            .setRequestMarshaller(ProtoUtils.marshaller(StringValue.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(BoolValue.getDefaultInstance()))
            .build();

    /** Execute a SPARQL {@code JSON} query and return the serialized JSON array in a {@link StringValue}. */
    public static final MethodDescriptor<StringValue, StringValue> QUERY_JSON_METHOD =
        MethodDescriptor.<StringValue, StringValue>newBuilder()
            .setType(MethodType.UNARY)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "QueryJson"))
            .setSafe(true)
            .setRequestMarshaller(ProtoUtils.marshaller(StringValue.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(StringValue.getDefaultInstance()))
            .build();

    /** Execute a SPARQL Update request. */
    public static final MethodDescriptor<StringValue, Empty> EXECUTE_UPDATE_METHOD =
        MethodDescriptor.<StringValue, Empty>newBuilder()
            .setType(MethodType.UNARY)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "ExecuteUpdate"))
            .setRequestMarshaller(ProtoUtils.marshaller(StringValue.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(Empty.getDefaultInstance()))
            .build();

    /** Apply a client-streamed sequence of {@link RDF_Patch_Row} messages to the dataset. */
    public static final MethodDescriptor<RDF_Patch_Row, Empty> APPLY_PATCH_METHOD =
        MethodDescriptor.<RDF_Patch_Row, Empty>newBuilder()
            .setType(MethodType.CLIENT_STREAMING)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "ApplyPatch"))
            .setRequestMarshaller(ProtoUtils.marshaller(RDF_Patch_Row.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(Empty.getDefaultInstance()))
            .build();

    /** Apply a batched {@link RDF_Patch} message to the dataset. */
    public static final MethodDescriptor<RDF_Patch, Empty> APPLY_PATCH_BATCH_METHOD =
        MethodDescriptor.<RDF_Patch, Empty>newBuilder()
            .setType(MethodType.UNARY)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "ApplyPatchBatch"))
            .setRequestMarshaller(ProtoUtils.marshaller(RDF_Patch.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(Empty.getDefaultInstance()))
            .build();

    /** Subscribe to live {@link RDF_Patch_Row} updates applied to the server dataset. */
    public static final MethodDescriptor<Empty, RDF_Patch_Row> SUBSCRIBE_PATCHES_METHOD =
        MethodDescriptor.<Empty, RDF_Patch_Row>newBuilder()
            .setType(MethodType.SERVER_STREAMING)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "SubscribePatches"))
            .setRequestMarshaller(ProtoUtils.marshaller(Empty.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(RDF_Patch_Row.getDefaultInstance()))
            .build();

    /** Begin a remote transaction on the server dataset, returning a transaction identifier. */
    public static final MethodDescriptor<StringValue, StringValue> BEGIN_TXN_METHOD =
        MethodDescriptor.<StringValue, StringValue>newBuilder()
            .setType(MethodType.UNARY)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "BeginTxn"))
            .setRequestMarshaller(ProtoUtils.marshaller(StringValue.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(StringValue.getDefaultInstance()))
            .build();

    /** Commit an active remote transaction identified by its transaction identifier. */
    public static final MethodDescriptor<StringValue, Empty> COMMIT_TXN_METHOD =
        MethodDescriptor.<StringValue, Empty>newBuilder()
            .setType(MethodType.UNARY)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "CommitTxn"))
            .setRequestMarshaller(ProtoUtils.marshaller(StringValue.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(Empty.getDefaultInstance()))
            .build();

    /** Abort an active remote transaction identified by its transaction identifier. */
    public static final MethodDescriptor<StringValue, Empty> ABORT_TXN_METHOD =
        MethodDescriptor.<StringValue, Empty>newBuilder()
            .setType(MethodType.UNARY)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "AbortTxn"))
            .setRequestMarshaller(ProtoUtils.marshaller(StringValue.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(Empty.getDefaultInstance()))
            .build();

    /** End an active remote transaction identified by its transaction identifier. */
    public static final MethodDescriptor<StringValue, Empty> END_TXN_METHOD =
        MethodDescriptor.<StringValue, Empty>newBuilder()
            .setType(MethodType.UNARY)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "EndTxn"))
            .setRequestMarshaller(ProtoUtils.marshaller(StringValue.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(Empty.getDefaultInstance()))
            .build();

    /** Promote an active remote transaction using {@code txnId + ":" + promoteMode}. */
    public static final MethodDescriptor<StringValue, BoolValue> PROMOTE_TXN_METHOD =
        MethodDescriptor.<StringValue, BoolValue>newBuilder()
            .setType(MethodType.UNARY)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "PromoteTxn"))
            .setRequestMarshaller(ProtoUtils.marshaller(StringValue.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(BoolValue.getDefaultInstance()))
            .build();

    private RDFGrpcProtocol() {}

    /**
     * Encode a graph name {@link Node} into an {@link RDF_Term}, mapping {@code null} or
     * default-graph markers to an undefined term.
     *
     * @param graphName the graph name node, or {@code null} for the default graph
     * @return the encoded {@link RDF_Term}
     */
    static RDF_Term encodeGraphNode(Node graphName) {
        if ( graphName == null || Quad.isDefaultGraph(graphName) )
            return UNDEF_TERM;
        return ProtobufConvert.convert(graphName, true);
    }

    /**
     * Decode an {@link RDF_Term} into a graph name {@link Node}, returning {@code null}
     * when the term represents the default graph.
     *
     * @param term the encoded graph term
     * @return the graph name {@link Node}, or {@code null} for the default graph
     */
    static Node decodeGraphNode(RDF_Term term) {
        if ( term == null || term.getTermCase() == RDF_Term.TermCase.TERM_NOT_SET || term.hasUndefined() )
            return null;
        Node n = ProtobufConvert.convert(term);
        if ( n == null || Quad.isDefaultGraph(n) )
            return null;
        return n;
    }

    /**
     * Decode the graph name {@link Node} from binary metadata bytes, or {@code null} for the default graph.
     *
     * @param bytes serialized {@link RDF_Term} bytes, or {@code null}
     * @return the decoded graph name {@link Node}, or {@code null} for the default graph
     */
    static Node decodeGraphNodeBytes(byte[] bytes) {
        if ( bytes == null || bytes.length == 0 )
            return null;
        try {
            RDF_Term term = RDF_Term.parseFrom(bytes);
            return decodeGraphNode(term);
        } catch (InvalidProtocolBufferException ex) {
            throw Status.INVALID_ARGUMENT.withDescription("Invalid graph term metadata").withCause(ex).asRuntimeException();
        }
    }

    /**
     * Encode a quad pattern {@code (g, s, p, o)} into an {@link RDF_Quad} message, mapping
     * {@code null} components to {@link Node#ANY}.
     *
     * @param g graph pattern node, or {@code null} for wildcard
     * @param s subject pattern node, or {@code null} for wildcard
     * @param p predicate pattern node, or {@code null} for wildcard
     * @param o object pattern node, or {@code null} for wildcard
     * @return the encoded {@link RDF_Quad} pattern
     */
    static RDF_Quad encodeQuadPattern(Node g, Node s, Node p, Node o) {
        Node gNode = (g == null) ? Node.ANY : g;
        Node sNode = (s == null) ? Node.ANY : s;
        Node pNode = (p == null) ? Node.ANY : p;
        Node oNode = (o == null) ? Node.ANY : o;
        return ProtobufConvert.convert(Quad.create(gNode, sNode, pNode, oNode), true);
    }

    /**
     * Convert a server-side exception into a gRPC {@link StatusRuntimeException} with a typed prefix.
     *
     * @param ex the exception thrown during request processing
     * @return the corresponding {@link StatusRuntimeException}
     */
    static StatusRuntimeException toStatusException(Throwable ex) {
        if ( ex instanceof StatusRuntimeException sre )
            return sre;
        String msg = ex.getMessage() != null ? ex.getMessage() : ex.getClass().getSimpleName();
        if ( ex instanceof QueryParseException ) {
            return Status.INVALID_ARGUMENT.withDescription(PREFIX_QUERY_PARSE_ERROR + msg).withCause(ex).asRuntimeException();
        }
        if ( ex instanceof QueryException ) {
            return Status.INVALID_ARGUMENT.withDescription(PREFIX_QUERY_ERROR + msg).withCause(ex).asRuntimeException();
        }
        if ( ex instanceof UpdateException ) {
            return Status.INVALID_ARGUMENT.withDescription(PREFIX_UPDATE_ERROR + msg).withCause(ex).asRuntimeException();
        }
        if ( ex instanceof JenaTransactionException ) {
            return Status.FAILED_PRECONDITION.withDescription(PREFIX_TXN_ERROR + msg).withCause(ex).asRuntimeException();
        }
        Log.warn(RDFGrpcProtocol.class, "Unhandled server exception in gRPC service", ex);
        return Status.INTERNAL.withDescription("Internal server error").withCause(ex).asRuntimeException();
    }

    /**
     * Translate a client-side {@link StatusRuntimeException} back into the appropriate Jena exception.
     *
     * @param sre the gRPC status exception
     * @return the translated runtime exception
     */
    static RuntimeException fromStatusException(StatusRuntimeException sre) {
        String desc = sre.getStatus().getDescription();
        if ( desc != null ) {
            if ( desc.startsWith(PREFIX_QUERY_PARSE_ERROR) ) {
                return new QueryParseException(desc.substring(PREFIX_QUERY_PARSE_ERROR.length()), -1, -1);
            }
            if ( desc.startsWith(PREFIX_QUERY_ERROR) ) {
                return new QueryException(desc.substring(PREFIX_QUERY_ERROR.length()), sre);
            }
            if ( desc.startsWith(PREFIX_UPDATE_ERROR) ) {
                return new UpdateException(desc.substring(PREFIX_UPDATE_ERROR.length()), sre);
            }
            if ( desc.startsWith(PREFIX_TXN_ERROR) ) {
                return new JenaTransactionException(desc.substring(PREFIX_TXN_ERROR.length()), sre);
            }
        }
        if ( sre.getStatus().getCode() == Status.Code.INVALID_ARGUMENT ) {
            return new QueryException(desc != null ? desc : "Invalid SPARQL request", sre);
        }
        return new ARQException(desc != null ? desc : sre.getMessage(), sre);
    }
}
