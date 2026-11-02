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

import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.ProtoUtils;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.graph.Node;
import org.apache.jena.grpc.wire.PB_RDFService.ApplyPatchRequest;
import org.apache.jena.grpc.wire.PB_RDFService.ApplyPatchResponse;
import org.apache.jena.grpc.wire.PB_RDFService.ExecuteQueryRequest;
import org.apache.jena.grpc.wire.PB_RDFService.ExecuteQueryResponse;
import org.apache.jena.grpc.wire.PB_RDFService.ExecuteUpdateRequest;
import org.apache.jena.grpc.wire.PB_RDFService.ExecuteUpdateResponse;
import org.apache.jena.grpc.wire.PB_RDFService.GraphTarget;
import org.apache.jena.grpc.wire.PB_RDFService.PromoteMode;
import org.apache.jena.grpc.wire.PB_RDFService.StreamRdfRequest;
import org.apache.jena.grpc.wire.PB_RDFService.StreamRdfResponse;
import org.apache.jena.grpc.wire.PB_RDFService.SubscribePatchesRequest;
import org.apache.jena.grpc.wire.PB_RDFService.SubscribePatchesResponse;
import org.apache.jena.grpc.wire.PB_RDFService.TransactionRequest;
import org.apache.jena.grpc.wire.PB_RDFService.TransactionResponse;
import org.apache.jena.grpc.wire.PB_RDFService.TransactionType;
import org.apache.jena.grpc.wire.PB_RDFService.WriteRdfRequest;
import org.apache.jena.grpc.wire.PB_RDFService.WriteRdfResponse;
import org.apache.jena.query.QueryException;
import org.apache.jena.query.QueryParseException;
import org.apache.jena.query.TxnType;
import org.apache.jena.riot.protobuf.ProtobufConvert;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Quad;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Term;
import org.apache.jena.sparql.ARQException;
import org.apache.jena.sparql.JenaTransactionException;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Transactional.Promote;
import org.apache.jena.update.UpdateException;

/**
 * gRPC service descriptors, metadata keys, and wire helpers for the Apache Jena
 * {@code org.apache.jena.grpc.v1.RdfDatasetService} stream-first protocol.
 */
public final class RDFGrpcProtocol {

    /** Full gRPC service name for {@link RDFDatasetGrpcService}. */
    public static final String SERVICE_NAME = "org.apache.jena.grpc.v1.RdfDatasetService";

    /** Error category values carried in gRPC response trailers. */
    static final String ERR_CATEGORY_QUERY_PARSE = "QUERY_PARSE";
    static final String ERR_CATEGORY_QUERY_EXEC = "QUERY_EXEC";
    static final String ERR_CATEGORY_UPDATE_EXEC = "UPDATE_EXEC";
    static final String ERR_CATEGORY_TRANSACTION = "TRANSACTION";

    /** Metadata header key carrying an active remote transaction identifier. */
    public static final Metadata.Key<String> META_TXN_ID =
        Metadata.Key.of("x-jena-txn-id", Metadata.ASCII_STRING_MARSHALLER);

    /** Trailer metadata key carrying the structured Jena error category on failure. */
    public static final Metadata.Key<String> META_ERROR_CATEGORY =
        Metadata.Key.of("x-jena-error-category", Metadata.ASCII_STRING_MARSHALLER);

    /** Server context key holding the active transaction identifier from request metadata. */
    static final Context.Key<String> CTX_TXN_ID = Context.key("jena-txn-id");

    /** Server-streaming RDF quad/triple/prefix read method. */
    public static final MethodDescriptor<StreamRdfRequest, StreamRdfResponse> STREAM_RDF_METHOD =
        MethodDescriptor.<StreamRdfRequest, StreamRdfResponse>newBuilder()
            .setType(MethodType.SERVER_STREAMING)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "StreamRdf"))
            .setSafe(true)
            .setRequestMarshaller(ProtoUtils.marshaller(StreamRdfRequest.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(StreamRdfResponse.getDefaultInstance()))
            .build();

    /** Client-streaming RDF write and graph management method. */
    public static final MethodDescriptor<WriteRdfRequest, WriteRdfResponse> WRITE_RDF_METHOD =
        MethodDescriptor.<WriteRdfRequest, WriteRdfResponse>newBuilder()
            .setType(MethodType.CLIENT_STREAMING)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "WriteRdf"))
            .setRequestMarshaller(ProtoUtils.marshaller(WriteRdfRequest.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(WriteRdfResponse.getDefaultInstance()))
            .build();

    /** Server-streaming SPARQL Query evaluation method. */
    public static final MethodDescriptor<ExecuteQueryRequest, ExecuteQueryResponse> EXECUTE_QUERY_METHOD =
        MethodDescriptor.<ExecuteQueryRequest, ExecuteQueryResponse>newBuilder()
            .setType(MethodType.SERVER_STREAMING)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "ExecuteQuery"))
            .setSafe(true)
            .setRequestMarshaller(ProtoUtils.marshaller(ExecuteQueryRequest.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(ExecuteQueryResponse.getDefaultInstance()))
            .build();

    /** Client-streaming SPARQL Update execution method. */
    public static final MethodDescriptor<ExecuteUpdateRequest, ExecuteUpdateResponse> EXECUTE_UPDATE_METHOD =
        MethodDescriptor.<ExecuteUpdateRequest, ExecuteUpdateResponse>newBuilder()
            .setType(MethodType.CLIENT_STREAMING)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "ExecuteUpdate"))
            .setRequestMarshaller(ProtoUtils.marshaller(ExecuteUpdateRequest.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(ExecuteUpdateResponse.getDefaultInstance()))
            .build();

    /** Client-streaming RDF Patch application method. */
    public static final MethodDescriptor<ApplyPatchRequest, ApplyPatchResponse> APPLY_PATCH_METHOD =
        MethodDescriptor.<ApplyPatchRequest, ApplyPatchResponse>newBuilder()
            .setType(MethodType.CLIENT_STREAMING)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "ApplyPatch"))
            .setRequestMarshaller(ProtoUtils.marshaller(ApplyPatchRequest.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(ApplyPatchResponse.getDefaultInstance()))
            .build();

    /** Server-streaming live RDF Patch subscription method. */
    public static final MethodDescriptor<SubscribePatchesRequest, SubscribePatchesResponse> SUBSCRIBE_PATCHES_METHOD =
        MethodDescriptor.<SubscribePatchesRequest, SubscribePatchesResponse>newBuilder()
            .setType(MethodType.SERVER_STREAMING)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "SubscribePatches"))
            .setRequestMarshaller(ProtoUtils.marshaller(SubscribePatchesRequest.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(SubscribePatchesResponse.getDefaultInstance()))
            .build();

    /** Bidirectional streaming transaction control session method. */
    public static final MethodDescriptor<TransactionRequest, TransactionResponse> TRANSACTION_METHOD =
        MethodDescriptor.<TransactionRequest, TransactionResponse>newBuilder()
            .setType(MethodType.BIDI_STREAMING)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "Transaction"))
            .setRequestMarshaller(ProtoUtils.marshaller(TransactionRequest.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(TransactionResponse.getDefaultInstance()))
            .build();

    private RDFGrpcProtocol() {}

    /**
     * Encode a graph name {@link Node} into a {@link GraphTarget}, mapping {@code null} or
     * default-graph markers to {@code default_graph = true}.
     *
     * @param graphName the graph name node, or {@code null} for the default graph
     * @return the encoded {@link GraphTarget}
     */
    static GraphTarget encodeGraphTarget(Node graphName) {
        if ( graphName == null || Quad.isDefaultGraph(graphName) ) {
            return GraphTarget.newBuilder().setDefaultGraph(true).build();
        }
        RDF_Term term = ProtobufConvert.convert(graphName, true);
        return GraphTarget.newBuilder().setGraphName(term).build();
    }

    /**
     * Decode a {@link GraphTarget} into a graph name {@link Node}, returning {@code null}
     * when the target represents the default graph.
     *
     * @param target the encoded graph target
     * @return the graph name {@link Node}, or {@code null} for the default graph
     */
    static Node decodeGraphTarget(GraphTarget target) {
        if ( target == null
                || target.getTargetCase() == GraphTarget.TargetCase.TARGET_NOT_SET
                || target.getDefaultGraph() ) {
            return null;
        }
        RDF_Term term = target.getGraphName();
        if ( term.getTermCase() == RDF_Term.TermCase.TERM_NOT_SET || term.hasUndefined() ) {
            return null;
        }
        Node n = ProtobufConvert.convert(term);
        if ( n == null || Quad.isDefaultGraph(n) ) {
            return null;
        }
        return n;
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
     * Convert a Jena {@link TxnType} to its Protobuf {@link TransactionType}.
     *
     * @param txnType the Jena transaction type
     * @return the wire transaction type
     */
    static TransactionType encodeTxnType(TxnType txnType) {
        if ( txnType == null )
            return TransactionType.TRANSACTION_TYPE_WRITE;
        return switch ( txnType ) {
            case READ -> TransactionType.TRANSACTION_TYPE_READ;
            case WRITE -> TransactionType.TRANSACTION_TYPE_WRITE;
            case READ_PROMOTE -> TransactionType.TRANSACTION_TYPE_READ_PROMOTE;
            case READ_COMMITTED_PROMOTE -> TransactionType.TRANSACTION_TYPE_READ_COMMITTED_PROMOTE;
        };
    }

    /**
     * Convert a wire {@link TransactionType} to its Jena {@link TxnType}.
     *
     * @param protoType the wire transaction type
     * @return the Jena transaction type
     */
    static TxnType decodeTxnType(TransactionType protoType) {
        if ( protoType == null )
            return TxnType.WRITE;
        return switch ( protoType ) {
            case TRANSACTION_TYPE_READ -> TxnType.READ;
            case TRANSACTION_TYPE_READ_PROMOTE -> TxnType.READ_PROMOTE;
            case TRANSACTION_TYPE_READ_COMMITTED_PROMOTE -> TxnType.READ_COMMITTED_PROMOTE;
            default -> TxnType.WRITE;
        };
    }

    /**
     * Convert a Jena {@link Promote} mode to its wire {@link PromoteMode}.
     *
     * @param promote the Jena promotion mode
     * @return the wire promotion mode
     */
    static PromoteMode encodePromoteMode(Promote promote) {
        if ( promote == Promote.READ_COMMITTED )
            return PromoteMode.PROMOTE_MODE_READ_COMMITTED;
        return PromoteMode.PROMOTE_MODE_ISOLATED;
    }

    /**
     * Convert a wire {@link PromoteMode} to its Jena {@link Promote} mode.
     *
     * @param protoMode the wire promotion mode
     * @return the Jena promotion mode
     */
    static Promote decodePromoteMode(PromoteMode protoMode) {
        if ( protoMode == PromoteMode.PROMOTE_MODE_READ_COMMITTED )
            return Promote.READ_COMMITTED;
        return Promote.ISOLATED;
    }

    /**
     * Convert a server-side exception into a gRPC {@link StatusRuntimeException} with structured error trailers.
     *
     * @param ex the exception thrown during request processing
     * @return the corresponding {@link StatusRuntimeException}
     */
    static StatusRuntimeException toStatusException(Throwable ex) {
        if ( ex instanceof StatusRuntimeException sre )
            return sre;
        String msg = ex.getMessage() != null ? ex.getMessage() : ex.getClass().getSimpleName();
        if ( ex instanceof QueryParseException ) {
            return statusWithCategory(Status.INVALID_ARGUMENT, msg, ex, ERR_CATEGORY_QUERY_PARSE);
        }
        if ( ex instanceof QueryException ) {
            return statusWithCategory(Status.INVALID_ARGUMENT, msg, ex, ERR_CATEGORY_QUERY_EXEC);
        }
        if ( ex instanceof UpdateException ) {
            return statusWithCategory(Status.INVALID_ARGUMENT, msg, ex, ERR_CATEGORY_UPDATE_EXEC);
        }
        if ( ex instanceof JenaTransactionException ) {
            return statusWithCategory(Status.FAILED_PRECONDITION, msg, ex, ERR_CATEGORY_TRANSACTION);
        }
        Log.warn(RDFGrpcProtocol.class, "Unhandled server exception in gRPC service", ex);
        return Status.INTERNAL.withDescription("Internal server error").withCause(ex).asRuntimeException();
    }

    private static StatusRuntimeException statusWithCategory(
            Status baseStatus, String description, Throwable cause, String category) {
        Metadata trailers = new Metadata();
        trailers.put(META_ERROR_CATEGORY, category);
        return baseStatus.withDescription(description).withCause(cause).asRuntimeException(trailers);
    }

    /**
     * Translate a client-side {@link StatusRuntimeException} back into the appropriate Jena exception.
     *
     * @param sre the gRPC status exception
     * @return the translated runtime exception
     */
    static RuntimeException fromStatusException(StatusRuntimeException sre) {
        String desc = sre.getStatus().getDescription();
        String msg = desc != null ? desc : sre.getMessage();
        Metadata trailers = sre.getTrailers();
        String category = trailers != null ? trailers.get(META_ERROR_CATEGORY) : null;
        if ( category != null ) {
            return switch ( category ) {
                case ERR_CATEGORY_QUERY_PARSE -> new QueryParseException(msg, -1, -1);
                case ERR_CATEGORY_QUERY_EXEC -> new QueryException(msg, sre);
                case ERR_CATEGORY_UPDATE_EXEC -> new UpdateException(msg, sre);
                case ERR_CATEGORY_TRANSACTION -> new JenaTransactionException(msg, sre);
                default -> new ARQException(msg, sre);
            };
        }
        if ( sre.getStatus().getCode() == Status.Code.INVALID_ARGUMENT ) {
            return new QueryException(desc != null ? desc : "Invalid SPARQL request", sre);
        }
        if ( sre.getStatus().getCode() == Status.Code.FAILED_PRECONDITION ) {
            return new JenaTransactionException(desc != null ? desc : "Transaction error", sre);
        }
        return new ARQException(msg, sre);
    }
}
