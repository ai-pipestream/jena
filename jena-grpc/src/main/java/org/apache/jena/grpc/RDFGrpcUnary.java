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
import java.util.function.Consumer;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCalls;
import org.apache.jena.grpc.RDFGrpcFrames.GetRdfFrame;
import org.apache.jena.grpc.RDFGrpcFrames.PutRdfFrame;
import org.apache.jena.grpc.wire.PB_RDFService.GetRdfRequest;
import org.apache.jena.grpc.wire.PB_RDFService.PutRdfResponse;
import org.apache.jena.grpc.wire.PB_RDFService.StreamRdfRequest;
import org.apache.jena.grpc.wire.PB_RDFService.WriteRdfHeader;
import org.apache.jena.riot.protobuf.ProtobufRDF;
import org.apache.jena.riot.system.StreamRDF;

/**
 * The unary RDF calls, {@code GetRdf} and {@code PutRdf}: one request message and one
 * response message, with the whole selection or the whole write inside. They are the
 * simplest calls to make from any gRPC client and they are what the streaming calls are
 * measured against. Both sides hold the complete message in memory, and the gRPC message
 * size limit (4 MiB unless raised on the server and the channel) bounds what they carry.
 */
public final class RDFGrpcUnary {

    private RDFGrpcUnary() {}

    /**
     * Fetch a selection in one message and send its rows to {@code dest}.
     *
     * @param channel channel to the server
     * @param selection the selection, as for {@code StreamRdf}
     * @param dest where the rows go; started and finished by this call
     */
    public static void getRdf(Channel channel, StreamRdfRequest selection, StreamRDF dest) {
        getRdf(channel, CallOptions.DEFAULT, selection, dest);
    }

    /** As {@link #getRdf(Channel, StreamRdfRequest, StreamRDF)} with call options. */
    public static void getRdf(Channel channel, CallOptions callOptions, StreamRdfRequest selection, StreamRDF dest) {
        GetRdfRequest request = GetRdfRequest.newBuilder().setSelection(selection).build();
        GetRdfFrame response;
        try {
            response = ClientCalls.blockingUnaryCall(channel, RDFGrpcProtocol.GET_RDF_METHOD, callOptions, request);
        } catch (StatusRuntimeException sre) {
            throw RDFGrpcProtocol.fromStatusException(sre);
        }
        dest.start();
        ProtobufRDF.framesToStreamRDF(dest).accept(response.rows());
        dest.finish();
    }

    /**
     * Write RDF in one message. {@code producer} is called with a {@link StreamRDF} that
     * collects the rows; the message is sent when it returns.
     *
     * @param channel channel to the server
     * @param header target, disposition and optional transaction of the write
     * @param producer sends the rows
     * @param encodeValues {@code true} to encode numeric literals by value
     * @param termTables {@code true} to use version 2 of RDF Protobuf
     * @return the server's summary of the write
     */
    public static PutRdfResponse putRdf(Channel channel, WriteRdfHeader header, Consumer<StreamRDF> producer,
                                        boolean encodeValues, boolean termTables) {
        return putRdf(channel, CallOptions.DEFAULT, header, producer, encodeValues, termTables);
    }

    /** As {@link #putRdf(Channel, WriteRdfHeader, Consumer, boolean, boolean)} with call options. */
    public static PutRdfResponse putRdf(Channel channel, CallOptions callOptions, WriteRdfHeader header,
                                        Consumer<StreamRDF> producer, boolean encodeValues, boolean termTables) {
        ByteBuffer[] rows = { null };
        if ( producer != null ) {
            StreamRDF encoder = ProtobufRDF.streamToFrames(frame -> rows[0] = frame, Integer.MAX_VALUE, encodeValues, termTables);
            encoder.start();
            producer.accept(encoder);
            encoder.finish();
        }
        PutRdfFrame request = new PutRdfFrame(header, rows[0]);
        try {
            return ClientCalls.blockingUnaryCall(channel, RDFGrpcProtocol.PUT_RDF_METHOD, callOptions, request);
        } catch (StatusRuntimeException sre) {
            throw RDFGrpcProtocol.fromStatusException(sre);
        }
    }
}
