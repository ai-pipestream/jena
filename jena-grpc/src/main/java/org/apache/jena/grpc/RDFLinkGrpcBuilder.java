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

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.apache.jena.riot.protobuf.ProtobufRDF;

/**
 * Builder for configuring and creating {@link RDFLinkGrpc} connections.
 */
public class RDFLinkGrpcBuilder {

    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final String ERR_CHANNEL_NULL = "channel must not be null";
    private static final String ERR_HOST_NULL = "host must not be null";
    private static final String ERR_IN_PROCESS_EMPTY = "inProcessName must not be null or blank";
    private static final String ERR_NO_TARGET = "Either channel, inProcessName, or positive port must be configured";

    private Channel channel;
    private boolean ownChannel = true;
    private String host = DEFAULT_HOST;
    private int port = -1;
    private String inProcessName;
    private boolean parseCheckQueries = true;
    private boolean parseCheckUpdates = true;
    private boolean encodeValues = true;
    private boolean termTables = true;
    private int maxInboundMessageSize = 0;
    private int batchSize = ProtobufRDF.DEFAULT_BATCH_SIZE;
    private Duration txnCommandTimeout;

    /**
     * Create a new {@link RDFLinkGrpcBuilder}.
     */
    public RDFLinkGrpcBuilder() {}

    /**
     * Use an existing gRPC {@link Channel}.
     *
     * @param channel the gRPC channel
     * @return this builder
     * @throws IllegalArgumentException Thrown if {@code channel} is {@code null}
     */
    public RDFLinkGrpcBuilder channel(Channel channel) {
        if ( channel == null )
            throw new IllegalArgumentException(ERR_CHANNEL_NULL);
        this.channel = channel;
        this.ownChannel = false;
        return this;
    }

    /**
     * Set whether {@link RDFLinkGrpc#close()} should shut down the underlying {@link io.grpc.ManagedChannel}.
     *
     * @param ownChannel {@code true} to shut down the channel when the link closes
     * @return this builder
     */
    public RDFLinkGrpcBuilder ownChannel(boolean ownChannel) {
        this.ownChannel = ownChannel;
        return this;
    }

    /**
     * Set the target host for Netty TCP connections.
     *
     * @param host the target hostname or IP address
     * @return this builder
     * @throws IllegalArgumentException Thrown if {@code host} is {@code null}
     */
    public RDFLinkGrpcBuilder host(String host) {
        if ( host == null )
            throw new IllegalArgumentException(ERR_HOST_NULL);
        this.host = host;
        return this;
    }

    /**
     * Set the target TCP port for Netty connections.
     *
     * @param port the target TCP port
     * @return this builder
     * @throws IllegalArgumentException Thrown if {@code port <= 0}
     */
    public RDFLinkGrpcBuilder port(int port) {
        if ( port <= 0 )
            throw new IllegalArgumentException("port must be positive: " + port);
        this.port = port;
        return this;
    }

    /**
     * Configure the link to connect to an in-process gRPC server with the given name.
     *
     * @param inProcessName the in-process server name
     * @return this builder
     * @throws IllegalArgumentException Thrown if {@code inProcessName} is {@code null} or blank
     */
    public RDFLinkGrpcBuilder inProcessName(String inProcessName) {
        if ( inProcessName == null || inProcessName.isBlank() )
            throw new IllegalArgumentException(ERR_IN_PROCESS_EMPTY);
        this.inProcessName = inProcessName;
        return this;
    }

    /**
     * Set whether both SPARQL queries and updates should be syntax-checked locally before sending.
     *
     * @param parseCheck {@code true} to syntax-check locally
     * @return this builder
     */
    public RDFLinkGrpcBuilder parseCheckSPARQL(boolean parseCheck) {
        this.parseCheckQueries = parseCheck;
        this.parseCheckUpdates = parseCheck;
        return this;
    }

    /**
     * Set whether SPARQL queries should be syntax-checked locally before sending.
     *
     * @param parseCheckQueries {@code true} to syntax-check queries locally
     * @return this builder
     */
    public RDFLinkGrpcBuilder parseCheckQueries(boolean parseCheckQueries) {
        this.parseCheckQueries = parseCheckQueries;
        return this;
    }

    /**
     * Set whether SPARQL updates should be syntax-checked locally before sending.
     *
     * @param parseCheckUpdates {@code true} to syntax-check updates locally
     * @return this builder
     */
    public RDFLinkGrpcBuilder parseCheckUpdates(boolean parseCheckUpdates) {
        this.parseCheckUpdates = parseCheckUpdates;
        return this;
    }

    /**
     * Set whether numeric literals should be encoded by value in {@code PB_RDF} terms.
     *
     * @param encodeValues {@code true} to encode numeric literals by value
     * @return this builder
     */
    public RDFLinkGrpcBuilder encodeValues(boolean encodeValues) {
        this.encodeValues = encodeValues;
        return this;
    }

    /**
     * Set whether RDF sent to the server uses version 2 of the RDF Protobuf encoding
     * (per-stream term tables and repeats). The default is {@code true}; {@code false}
     * writes every term in full, as version 1 does.
     *
     * @param termTables {@code true} for version 2
     * @return this builder
     */
    public RDFLinkGrpcBuilder termTables(boolean termTables) {
        this.termTables = termTables;
        return this;
    }

    /**
     * Set the batch size for batched {@code RDF_Stream} messages.
     *
     * @param batchSize the batch size
     * @return this builder
     * @throws IllegalArgumentException Thrown if {@code batchSize <= 0}
     */
    public RDFLinkGrpcBuilder batchSize(int batchSize) {
        if ( batchSize <= 0 )
            throw new IllegalArgumentException("batchSize must be positive: " + batchSize);
        this.batchSize = batchSize;
        return this;
    }

    /**
     * Set the maximum time to wait for the server's response to a transaction command
     * ({@code BEGIN}, {@code PROMOTE}, {@code COMMIT}, {@code ABORT}, {@code END}).
     * By default there is no limit, so {@code begin(WRITE)} blocks until the server grants
     * the write lock, as a local Jena {@code begin} does. When the timeout elapses the
     * transaction stream is cancelled and a {@code JenaTransactionException} is thrown; for a
     * {@code COMMIT} the exception states that the outcome is unknown.
     *
     * @param txnCommandTimeout the timeout, or {@code null} to wait without limit
     * @return this builder
     * @throws IllegalArgumentException Thrown if {@code txnCommandTimeout} is zero or negative
     */
    public RDFLinkGrpcBuilder txnCommandTimeout(Duration txnCommandTimeout) {
        if ( txnCommandTimeout != null && (txnCommandTimeout.isNegative() || txnCommandTimeout.isZero()) )
            throw new IllegalArgumentException("txnCommandTimeout must be positive: " + txnCommandTimeout);
        this.txnCommandTimeout = txnCommandTimeout;
        return this;
    }

    /**
     * Set the largest response message the client accepts, in bytes, when this builder
     * creates the channel. The gRPC default is 4 MiB, which bounds what a unary
     * {@code GetRdf} can return. {@code 0} keeps the default. Ignored for a channel passed
     * to {@link #channel(Channel)}.
     *
     * @param maxInboundMessageSize largest response message in bytes, or {@code 0} for the gRPC default
     * @return this builder
     * @throws IllegalArgumentException Thrown if {@code maxInboundMessageSize < 0}
     */
    public RDFLinkGrpcBuilder maxInboundMessageSize(int maxInboundMessageSize) {
        if ( maxInboundMessageSize < 0 )
            throw new IllegalArgumentException("maxInboundMessageSize must be non-negative: " + maxInboundMessageSize);
        this.maxInboundMessageSize = maxInboundMessageSize;
        return this;
    }

    /**
     * Build the configured {@link RDFLinkGrpc}.
     *
     * @return the constructed {@link RDFLinkGrpc}
     * @throws IllegalArgumentException Thrown if no channel, in-process name, or TCP port was configured
     */
    public RDFLinkGrpc build() {
        Channel targetChannel = channel;
        boolean shouldOwn = ownChannel;
        if ( targetChannel == null ) {
            ManagedChannelBuilder<?> channelBuilder;
            if ( inProcessName != null ) {
                channelBuilder = InProcessChannelBuilder.forName(inProcessName).directExecutor();
            } else if ( port > 0 ) {
                channelBuilder = NettyChannelBuilder.forAddress(host, port);
            } else {
                throw new IllegalArgumentException(ERR_NO_TARGET);
            }
            if ( maxInboundMessageSize > 0 )
                channelBuilder.maxInboundMessageSize(maxInboundMessageSize);
            targetChannel = channelBuilder.usePlaintext().build();
            shouldOwn = true;
        }
        return new RDFLinkGrpc(
            targetChannel,
            shouldOwn,
            parseCheckQueries,
            parseCheckUpdates,
            encodeValues,
            termTables,
            batchSize,
            txnCommandTimeout);
    }
}
