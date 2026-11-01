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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import org.apache.jena.grpc.wire.PB_RDFService.ExecuteUpdateRequest;
import org.apache.jena.grpc.wire.PB_RDFService.ExecuteUpdateResponse;
import org.apache.jena.sparql.ARQException;
import org.apache.jena.sparql.exec.UpdateExec;
import org.apache.jena.sparql.util.Context;

/**
 * {@link UpdateExec} implementation executing SPARQL Updates over a gRPC {@link Channel}
 * using the client-streaming {@code ExecuteUpdate} RPC.
 */
public class UpdateExecGrpc implements UpdateExec {

    private final Channel channel;
    private final Supplier<CallOptions> callOptionsSupplier;
    private final String updateString;
    private final Context context;
    private final long timeout;
    private final TimeUnit timeoutUnit;

    UpdateExecGrpc(
            Channel channel,
            Supplier<CallOptions> callOptionsSupplier,
            String updateString,
            Context context,
            long timeout,
            TimeUnit timeoutUnit) {
        this.channel = channel;
        this.callOptionsSupplier = callOptionsSupplier;
        this.updateString = updateString;
        this.context = context;
        this.timeout = timeout;
        this.timeoutUnit = timeoutUnit;
    }

    /** {@inheritDoc} */
    @Override
    public Context getContext() {
        return context;
    }

    /** {@inheritDoc} */
    @Override
    public void execute() {
        CallOptions opts = callOptionsSupplier.get();
        if ( timeout > 0 && timeoutUnit != null ) {
            opts = opts.withDeadlineAfter(timeout, timeoutUnit);
        }
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> errorRef = new AtomicReference<>();
        ClientCall<ExecuteUpdateRequest, ExecuteUpdateResponse> call =
            channel.newCall(RDFGrpcProtocol.EXECUTE_UPDATE_METHOD, opts);
        StreamObserver<ExecuteUpdateRequest> reqObs = ClientCalls.asyncClientStreamingCall(
            call,
            new StreamObserver<>() {
                @Override
                public void onNext(ExecuteUpdateResponse value) {}

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
            reqObs.onNext(ExecuteUpdateRequest.newBuilder().setUpdate(updateString).build());
            reqObs.onCompleted();
            latch.await();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            call.cancel("Interrupted during SPARQL update", ex);
            throw new ARQException("Interrupted during SPARQL update", ex);
        } catch (RuntimeException ex) {
            reqObs.onError(ex);
            throw ex;
        }
        Throwable err = errorRef.get();
        if ( err != null ) {
            if ( err instanceof StatusRuntimeException sre ) {
                throw RDFGrpcProtocol.fromStatusException(sre);
            }
            throw new ARQException(err.getMessage(), err);
        }
    }
}
