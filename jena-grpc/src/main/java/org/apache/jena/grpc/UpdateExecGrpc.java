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

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.protobuf.StringValue;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCalls;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.exec.UpdateExec;
import org.apache.jena.sparql.util.Context;

/**
 * {@link UpdateExec} implementation executing SPARQL Updates over a gRPC {@link Channel}.
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
        try {
            CallOptions opts = callOptionsSupplier.get();
            if ( timeout > 0 && timeoutUnit != null ) {
                opts = opts.withDeadlineAfter(timeout, timeoutUnit);
            }
            ClientCalls.blockingUnaryCall(
                channel, RDFGrpcProtocol.EXECUTE_UPDATE_METHOD, opts, StringValue.of(updateString));
        } catch (StatusRuntimeException sre) {
            throw RDFGrpcProtocol.fromStatusException(sre);
        }
    }
}
