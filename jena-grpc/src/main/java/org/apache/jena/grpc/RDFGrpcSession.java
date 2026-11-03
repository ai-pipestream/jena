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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import org.apache.jena.grpc.RDFGrpcFrames.SessionRequest;
import org.apache.jena.grpc.RDFGrpcFrames.SessionResponse;
import org.apache.jena.grpc.wire.PB_RDFService.StreamRdfRequest;
import org.apache.jena.grpc.wire.PB_RDFService.WriteRdfHeader;
import org.apache.jena.grpc.wire.PB_RDFService.WriteRdfResponse;
import org.apache.jena.riot.protobuf.ProtobufRDF;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.ARQException;

/**
 * Client side of the {@code RdfSession} call: reads and writes of RDF over one long-lived
 * bidirectional gRPC stream. Operations are pipelined; a caller can start the next one
 * without waiting for the previous one to complete, and each completes through its own
 * future. The server runs reads in the order it receives them and keeps a write open until
 * its {@link Writer#finish()}. One write is open at a time (a write holds the dataset's
 * write lock until it ends, unless it names a remote transaction), so finish a writer
 * before starting the next; waiting for its result is not required.
 * <p>
 * Rows of a read are delivered to the destination {@link StreamRDF} on a gRPC thread, so
 * the destination must not depend on the calling thread. A failure of any operation fails
 * the stream and every operation still pending on it; the session is then unusable.
 * <p>
 * {@link #close()} half-closes the stream and waits for the server to finish the
 * pending operations.
 */
public final class RDFGrpcSession implements AutoCloseable {

    private final ClientCall<SessionRequest, SessionResponse> call;
    private final StreamObserver<SessionRequest> requests;
    private final boolean encodeValues;
    private final boolean termTables;
    private final int batchSize;
    private final AtomicLong nextOpId = new AtomicLong(1);
    private final Map<Long, Op> ops = new ConcurrentHashMap<>();
    private final CompletableFuture<Void> finished = new CompletableFuture<>();
    private volatile Throwable failure = null;
    private boolean closed = false;
    private Writer openWriter = null;

    private sealed interface Op {}
    private record ReadOp(StreamRDF dest, Consumer<ByteBuffer> rows, CompletableFuture<Void> done) implements Op {}
    private record WriteOp(CompletableFuture<WriteRdfResponse> done) implements Op {}

    /**
     * Open a session with the default options: values encoded, term tables (version 2
     * of RDF Protobuf) and {@link ProtobufRDF#DEFAULT_BATCH_SIZE} rows per frame.
     *
     * @param channel channel to a server that offers {@code RdfSession}
     * @return the open session
     */
    public static RDFGrpcSession open(Channel channel) {
        return open(channel, CallOptions.DEFAULT, true, true, ProtobufRDF.DEFAULT_BATCH_SIZE);
    }

    /**
     * Open a session.
     *
     * @param channel channel to a server that offers {@code RdfSession}
     * @param callOptions options of the underlying call
     * @param encodeValues {@code true} to encode numeric literals by value in written rows
     * @param termTables {@code true} to write rows with version 2 of RDF Protobuf
     * @param batchSize rows per frame of a write
     * @return the open session
     */
    public static RDFGrpcSession open(Channel channel, CallOptions callOptions, boolean encodeValues, boolean termTables, int batchSize) {
        if ( channel == null )
            throw new IllegalArgumentException("channel must not be null");
        if ( batchSize <= 0 )
            throw new IllegalArgumentException("batchSize must be positive: " + batchSize);
        return new RDFGrpcSession(channel, callOptions, encodeValues, termTables, batchSize);
    }

    private RDFGrpcSession(Channel channel, CallOptions callOptions, boolean encodeValues, boolean termTables, int batchSize) {
        this.encodeValues = encodeValues;
        this.termTables = termTables;
        this.batchSize = batchSize;
        this.call = channel.newCall(RDFGrpcProtocol.RDF_SESSION_METHOD, callOptions);
        this.requests = ClientCalls.asyncBidiStreamingCall(call, new StreamObserver<>() {
            @Override
            public void onNext(SessionResponse response) {
                event(response);
            }

            @Override
            public void onError(Throwable t) {
                fail(t);
            }

            @Override
            public void onCompleted() {
                finish();
            }
        });
    }

    /**
     * Start a read. The rows of the selection are sent to {@code dest} as they arrive, on a
     * gRPC thread; {@code dest.start()} is called now and {@code dest.finish()} when the
     * server has sent the last row.
     *
     * @param request the selection, as for {@code StreamRdf}
     * @param dest where the rows go
     * @return a future that completes after {@code dest.finish()}, or fails with the error of the read
     */
    public CompletableFuture<Void> read(StreamRdfRequest request, StreamRDF dest) {
        long opId = nextOpId.getAndIncrement();
        CompletableFuture<Void> done = new CompletableFuture<>();
        ops.put(opId, new ReadOp(dest, ProtobufRDF.framesToStreamRDF(dest), done));
        dest.start();
        send(SessionRequest.read(opId, request));
        return done;
    }

    /**
     * Start a write. The returned {@link Writer} is a {@link StreamRDF}: call
     * {@link Writer#start()}, send triples, quads and prefixes, then {@link Writer#finish()};
     * {@link Writer#result()} completes when the server has applied the write.
     *
     * @param header target, disposition and optional transaction of the write
     * @return the writer
     */
    public Writer writer(WriteRdfHeader header) {
        return new Writer(nextOpId.getAndIncrement(), header);
    }

    /**
     * Run one write: {@code producer} is called with a started {@link StreamRDF} and the
     * write is finished when it returns.
     *
     * @param header target, disposition and optional transaction of the write
     * @param producer sends the rows
     * @return a future of the server's summary of the write
     */
    public CompletableFuture<WriteRdfResponse> write(WriteRdfHeader header, Consumer<StreamRDF> producer) {
        Writer writer = writer(header);
        writer.start();
        producer.accept(writer);
        writer.finish();
        return writer.result();
    }

    /** {@code true} once the stream has failed or has been closed. */
    public boolean isOpen() {
        return failure == null && !finished.isDone();
    }

    /**
     * Half-close the stream and wait for the server to finish every pending operation.
     *
     * @throws ARQException if the stream failed, with the cause of the failure
     */
    @Override
    public void close() {
        synchronized (this) {
            if ( closed )
                return;
            closed = true;
            if ( failure == null )
                requests.onCompleted();
        }
        try {
            finished.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            call.cancel("Interrupted", ex);
            throw new ARQException("Interrupted while closing the gRPC session", ex);
        } catch (ExecutionException ex) {
            throw translate(ex.getCause());
        }
    }

    /**
     * Cancel the stream. Pending operations fail; writes still open on the server are
     * rolled back.
     */
    public void cancel() {
        synchronized (this) {
            closed = true;
        }
        call.cancel("Cancelled by the client", null);
    }

    private synchronized void send(SessionRequest request) {
        if ( closed )
            throw new ARQException("The gRPC session is closed");
        Throwable t = failure;
        if ( t != null )
            throw translate(t);
        requests.onNext(request);
    }

    private void event(SessionResponse response) {
        long opId = response.opId();
        Op op = ops.get(opId);
        if ( op == null )
            return;
        if ( response.readRows() != null ) {
            if ( op instanceof ReadOp read )
                read.rows.accept(response.readRows());
        } else if ( response.readEnd() ) {
            if ( op instanceof ReadOp read ) {
                ops.remove(opId);
                read.dest.finish();
                read.done.complete(null);
            }
        } else if ( response.writeDone() != null ) {
            if ( op instanceof WriteOp write ) {
                ops.remove(opId);
                write.done.complete(response.writeDone());
            }
        }
    }

    private void fail(Throwable t) {
        failure = t;
        RuntimeException ex = translate(t);
        failPending(ex);
        finished.completeExceptionally(ex);
    }

    private void finish() {
        failPending(new ARQException("The gRPC session ended before the operation completed"));
        finished.complete(null);
    }

    private void failPending(RuntimeException ex) {
        List<Op> pending = new ArrayList<>(ops.values());
        ops.clear();
        for ( Op op : pending ) {
            if ( op instanceof ReadOp read )
                read.done.completeExceptionally(ex);
            else if ( op instanceof WriteOp write )
                write.done.completeExceptionally(ex);
        }
    }

    private static RuntimeException translate(Throwable t) {
        if ( t instanceof CompletionException ce && ce.getCause() != null )
            t = ce.getCause();
        if ( t instanceof StatusRuntimeException sre )
            return RDFGrpcProtocol.fromStatusException(sre);
        if ( t instanceof RuntimeException re )
            return re;
        return new ARQException(t.getMessage(), t);
    }

    /** One write on the session, used as a {@link StreamRDF}. */
    public final class Writer implements StreamRDF {
        private final long opId;
        private final WriteRdfHeader header;
        private final StreamRDF encoder;
        private final CompletableFuture<WriteRdfResponse> result = new CompletableFuture<>();

        private Writer(long opId, WriteRdfHeader header) {
            this.opId = opId;
            this.header = header;
            this.encoder = ProtobufRDF.streamToFrames(
                frame -> send(SessionRequest.writeRows(opId, frame)),
                batchSize,
                encodeValues,
                termTables);
        }

        /** The server's summary of the write; completes after {@link #finish()} once the write is applied. */
        public CompletableFuture<WriteRdfResponse> result() {
            return result;
        }

        @Override
        public void start() {
            synchronized (RDFGrpcSession.this) {
                if ( openWriter != null )
                    throw new ARQException("A write is already open on this session; finish it before starting another");
                openWriter = this;
            }
            ops.put(opId, new WriteOp(result));
            send(SessionRequest.write(opId, header));
            encoder.start();
        }

        @Override
        public void triple(org.apache.jena.graph.Triple triple) {
            encoder.triple(triple);
        }

        @Override
        public void quad(org.apache.jena.sparql.core.Quad quad) {
            encoder.quad(quad);
        }

        @Override
        public void base(String base) {
            encoder.base(base);
        }

        @Override
        public void prefix(String prefix, String iri) {
            encoder.prefix(prefix, iri);
        }

        @Override
        public void version(String version) {
            encoder.version(version);
        }

        @Override
        public void finish() {
            synchronized (RDFGrpcSession.this) {
                if ( openWriter == this )
                    openWriter = null;
            }
            encoder.finish();
            send(SessionRequest.writeEnd(opId));
        }
    }
}
