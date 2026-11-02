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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import io.grpc.CallOptions;
import io.grpc.Channel;
import org.apache.jena.graph.Node;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Syntax;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.exec.UpdateExec;
import org.apache.jena.sparql.exec.UpdateExecBuilder;
import org.apache.jena.sparql.syntax.syntaxtransform.UpdateTransformOps;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.sparql.util.ContextAccumulator;
import org.apache.jena.sparql.util.Symbol;
import org.apache.jena.update.Update;
import org.apache.jena.update.UpdateException;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;

/**
 * {@link UpdateExecBuilder} for constructing {@link UpdateExecGrpc} instances over a gRPC {@link Channel}.
 */
public class UpdateExecGrpcBuilder implements UpdateExecBuilder {

    private static final String DELIMITER = "\n;\n";
    private static final String ERR_CHANNEL_NULL = "channel must not be null";
    private static final String ERR_CALL_OPTS_NULL = "callOptionsSupplier must not be null";
    private static final String ERR_NO_UPDATE = "No update for UpdateExecGrpc";

    private final Channel channel;
    private final Supplier<CallOptions> callOptionsSupplier;
    private boolean parseCheck;
    private final List<Update> parsedUpdates = new ArrayList<>();
    private final List<String> rawUpdateStrings = new ArrayList<>();
    private final ContextAccumulator contextAcc = ContextAccumulator.newBuilder(ARQ::getContext);
    private final Map<Var, Node> substitutionMap = new HashMap<>();
    private long timeout = -1L;
    private TimeUnit timeoutUnit;

    /**
     * Create a builder targeting {@code channel} with the supplied call options provider and parse-check setting.
     *
     * @param channel the gRPC channel
     * @param callOptionsSupplier supplier of per-call {@link CallOptions}
     * @param parseCheck {@code true} to validate update strings locally when added
     * @throws IllegalArgumentException Thrown if {@code channel} or {@code callOptionsSupplier} is {@code null}
     */
    public UpdateExecGrpcBuilder(Channel channel, Supplier<CallOptions> callOptionsSupplier, boolean parseCheck) {
        if ( channel == null )
            throw new IllegalArgumentException(ERR_CHANNEL_NULL);
        if ( callOptionsSupplier == null )
            throw new IllegalArgumentException(ERR_CALL_OPTS_NULL);
        this.channel = channel;
        this.callOptionsSupplier = callOptionsSupplier;
        this.parseCheck = parseCheck;
    }

    /** {@inheritDoc} */
    @Override
    public UpdateExecGrpcBuilder update(UpdateRequest request) {
        Objects.requireNonNull(request);
        parsedUpdates.addAll(request.getOperations());
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public UpdateExecGrpcBuilder update(Update update) {
        Objects.requireNonNull(update);
        parsedUpdates.add(update);
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public UpdateExecGrpcBuilder update(String updateString) {
        Objects.requireNonNull(updateString);
        if ( parseCheck ) {
            UpdateRequest req = UpdateFactory.create(updateString, Syntax.syntaxARQ);
            parsedUpdates.addAll(req.getOperations());
        } else {
            rawUpdateStrings.add(updateString);
        }
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public UpdateExecGrpcBuilder parseCheck(boolean parseCheck) {
        this.parseCheck = parseCheck;
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public UpdateExecGrpcBuilder set(Symbol symbol, Object value) {
        contextAcc.set(symbol, value);
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public UpdateExecGrpcBuilder set(Symbol symbol, boolean value) {
        contextAcc.set(symbol, value);
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public UpdateExecGrpcBuilder context(Context context) {
        if ( context != null ) {
            contextAcc.context(context);
        }
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public UpdateExecGrpcBuilder substitution(Binding binding) {
        Objects.requireNonNull(binding);
        binding.forEach(substitutionMap::put);
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public UpdateExecGrpcBuilder substitution(Var var, Node value) {
        Objects.requireNonNull(var);
        Objects.requireNonNull(value);
        substitutionMap.put(var, value);
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public UpdateExecGrpcBuilder timeout(long value, TimeUnit timeUnit) {
        if ( value < 0 ) {
            this.timeout = -1L;
            this.timeoutUnit = null;
        } else {
            this.timeout = value;
            this.timeoutUnit = Objects.requireNonNull(timeUnit);
        }
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public UpdateExec build() {
        if ( parsedUpdates.isEmpty() && rawUpdateStrings.isEmpty() )
            throw new UpdateException(ERR_NO_UPDATE);

        String finalUpdateString;
        if ( parseCheck || !substitutionMap.isEmpty() || rawUpdateStrings.isEmpty() ) {
            UpdateRequest combined = new UpdateRequest();
            parsedUpdates.forEach(combined::add);
            for ( String raw : rawUpdateStrings ) {
                UpdateRequest req = UpdateFactory.create(raw, Syntax.syntaxARQ);
                req.getOperations().forEach(combined::add);
            }
            if ( !substitutionMap.isEmpty() ) {
                combined = UpdateTransformOps.transform(combined, substitutionMap);
            }
            finalUpdateString = combined.toString();
        } else {
            List<String> parts = new ArrayList<>();
            for ( Update u : parsedUpdates ) {
                parts.add(new UpdateRequest(u).toString());
            }
            parts.addAll(rawUpdateStrings);
            finalUpdateString = String.join(DELIMITER, parts);
        }

        return new UpdateExecGrpc(
            channel,
            callOptionsSupplier,
            finalUpdateString,
            contextAcc.context(),
            timeout,
            timeoutUnit);
    }
}
