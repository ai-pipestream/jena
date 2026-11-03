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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.apache.jena.riot.protobuf.ProtobufRDF;
import org.apache.jena.sparql.ARQException;
import org.apache.jena.sparql.core.DatasetGraph;

/**
 * Jetty-free gRPC server hosting an {@link RDFDatasetGrpcService} over either
 * Netty TCP sockets or an in-process gRPC transport.
 */
public class RDFGrpcServer implements AutoCloseable {

    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final String ERR_DSG_NULL = "dataset must not be null";
    private static final String ERR_IN_PROCESS_EMPTY = "inProcessName must not be null or blank";
    private static final String ERR_HOST_NULL = "host must not be null";

    private final Server server;
    private final RDFDatasetGrpcService service;

    private RDFGrpcServer(Server server, RDFDatasetGrpcService service) {
        this.server = server;
        this.service = service;
    }

    /**
     * Create a new {@link Builder} for configuring an {@link RDFGrpcServer}.
     *
     * @return a new builder instance
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Start listening for incoming gRPC requests.
     *
     * @return this server instance
     * @throws ARQException Thrown if the underlying gRPC server fails to start
     */
    public RDFGrpcServer start() {
        try {
            server.start();
            return this;
        } catch (IOException ex) {
            throw new ARQException("Failed to start RDFGrpcServer", ex);
        }
    }

    /**
     * Return the bound TCP port, or {@code -1} if running on an in-process transport.
     *
     * @return the bound TCP port or {@code -1}
     */
    public int getPort() {
        return server.getPort();
    }

    /**
     * Return the hosted {@link RDFDatasetGrpcService}.
     *
     * @return the gRPC dataset service
     */
    public RDFDatasetGrpcService getService() {
        return service;
    }

    /**
     * Initiate an orderly shutdown of the gRPC server.
     */
    public void stop() {
        service.close();
        server.shutdown();
        try {
            if ( !server.awaitTermination(5, TimeUnit.SECONDS) ) {
                server.shutdownNow();
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            server.shutdownNow();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        stop();
    }

    /**
     * Builder for {@link RDFGrpcServer}.
     */
    public static final class Builder {
        private DatasetGraph dataset;
        private String host = DEFAULT_HOST;
        private int port = 0;
        private String inProcessName;
        private boolean encodeValues = true;
        private boolean termTables = true;
        private int maxInboundMessageSize = 0;
        private int batchSize = ProtobufRDF.DEFAULT_BATCH_SIZE;
        private Duration txnIdleTimeout = RDFDatasetGrpcService.DEFAULT_TXN_IDLE_TIMEOUT;

        private Builder() {}

        /**
         * Set the backing {@link DatasetGraph} to serve.
         *
         * @param dataset the dataset graph
         * @return this builder
         * @throws IllegalArgumentException Thrown if {@code dataset} is {@code null}
         */
        public Builder dataset(DatasetGraph dataset) {
            if ( dataset == null )
                throw new IllegalArgumentException(ERR_DSG_NULL);
            this.dataset = dataset;
            return this;
        }

        /**
         * Set the bind host address for Netty TCP transport (defaults to {@code 127.0.0.1}).
         *
         * @param host the bind host
         * @return this builder
         * @throws IllegalArgumentException Thrown if {@code host} is {@code null}
         */
        public Builder host(String host) {
            if ( host == null )
                throw new IllegalArgumentException(ERR_HOST_NULL);
            this.host = host;
            return this;
        }

        /**
         * Set the TCP port for Netty transport (use {@code 0} for an ephemeral port).
         *
         * @param port the TCP port
         * @return this builder
         * @throws IllegalArgumentException Thrown if {@code port < 0}
         */
        public Builder port(int port) {
            if ( port < 0 )
                throw new IllegalArgumentException("port must be non-negative: " + port);
            this.port = port;
            return this;
        }

        /**
         * Configure the server to use the in-process transport with the given server name
         * instead of a TCP socket.
         *
         * @param inProcessName the in-process server name
         * @return this builder
         * @throws IllegalArgumentException Thrown if {@code inProcessName} is {@code null} or blank
         */
        public Builder inProcessName(String inProcessName) {
            if ( inProcessName == null || inProcessName.isBlank() )
                throw new IllegalArgumentException(ERR_IN_PROCESS_EMPTY);
            this.inProcessName = inProcessName;
            return this;
        }

        /**
         * Set whether numeric literals should be encoded by value in {@code PB_RDF} terms.
         *
         * @param encodeValues {@code true} to encode numeric values
         * @return this builder
         */
        public Builder encodeValues(boolean encodeValues) {
            this.encodeValues = encodeValues;
            return this;
        }

        /**
         * Set whether RDF is sent with version 2 of the RDF Protobuf encoding: per-stream
         * term tables and repeats, which is much smaller and faster to decode. The default
         * is {@code true}; {@code false} writes every term in full, as version 1 does.
         *
         * @param termTables {@code true} for version 2
         * @return this builder
         */
        public Builder termTables(boolean termTables) {
            this.termTables = termTables;
            return this;
        }

        /**
         * Set the batch size for batched {@code RDF_Stream} responses.
         *
         * @param batchSize the batch size
         * @return this builder
         * @throws IllegalArgumentException Thrown if {@code batchSize <= 0}
         */
        /**
         * Set the largest request message the server accepts, in bytes. The gRPC default is
         * 4 MiB, which bounds what a unary {@code PutRdf} can carry; the streaming calls are
         * unaffected because their frames are one batch each. {@code 0} keeps the default.
         *
         * @param maxInboundMessageSize largest request message in bytes, or {@code 0} for the gRPC default
         * @return this builder
         * @throws IllegalArgumentException Thrown if {@code maxInboundMessageSize < 0}
         */
        public Builder maxInboundMessageSize(int maxInboundMessageSize) {
            if ( maxInboundMessageSize < 0 )
                throw new IllegalArgumentException("maxInboundMessageSize must be non-negative: " + maxInboundMessageSize);
            this.maxInboundMessageSize = maxInboundMessageSize;
            return this;
        }

        public Builder batchSize(int batchSize) {
            if ( batchSize <= 0 )
                throw new IllegalArgumentException("batchSize must be positive: " + batchSize);
            this.batchSize = batchSize;
            return this;
        }

        /**
         * Set how long a remote transaction, or a non-transactional streaming write call waiting
         * for its next client frame, may stay idle before the server aborts and ends it
         * (defaults to {@link RDFDatasetGrpcService#DEFAULT_TXN_IDLE_TIMEOUT}).
         *
         * @param txnIdleTimeout the idle timeout
         * @return this builder
         * @throws IllegalArgumentException Thrown if {@code txnIdleTimeout} is {@code null} or not positive
         */
        public Builder txnIdleTimeout(Duration txnIdleTimeout) {
            if ( txnIdleTimeout == null || txnIdleTimeout.isNegative() || txnIdleTimeout.isZero() )
                throw new IllegalArgumentException("txnIdleTimeout must be positive: " + txnIdleTimeout);
            this.txnIdleTimeout = txnIdleTimeout;
            return this;
        }

        /**
         * Build the {@link RDFGrpcServer}.
         *
         * @return the unstarted {@link RDFGrpcServer}
         * @throws IllegalArgumentException Thrown if no {@link DatasetGraph} was configured
         */
        public RDFGrpcServer build() {
            if ( dataset == null )
                throw new IllegalArgumentException(ERR_DSG_NULL);
            RDFDatasetGrpcService service = new RDFDatasetGrpcService(dataset, encodeValues, termTables, batchSize, txnIdleTimeout);
            ServerBuilder<?> serverBuilder;
            if ( inProcessName != null ) {
                serverBuilder = InProcessServerBuilder.forName(inProcessName).directExecutor();
            } else {
                serverBuilder = NettyServerBuilder.forAddress(new InetSocketAddress(host, port));
            }
            if ( maxInboundMessageSize > 0 )
                serverBuilder.maxInboundMessageSize(maxInboundMessageSize);
            Server server = serverBuilder.addService(service).build();
            return new RDFGrpcServer(server, service);
        }
    }
}
