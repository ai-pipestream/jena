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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import io.grpc.CallOptions;
import io.grpc.Channel;
import org.apache.jena.graph.Node;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryException;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.exec.QueryExec;
import org.apache.jena.sparql.exec.QueryExecBuilder;
import org.apache.jena.sparql.exec.QueryExecMod;
import org.apache.jena.sparql.syntax.syntaxtransform.QueryTransformOps;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.sparql.util.ContextAccumulator;
import org.apache.jena.sparql.util.Symbol;

/**
 * {@link QueryExecBuilder} for constructing {@link QueryExecGrpc} instances over a gRPC {@link Channel}.
 */
public class QueryExecGrpcBuilder implements QueryExecMod, QueryExecBuilder {

    private static final String ERR_CHANNEL_NULL = "channel must not be null";
    private static final String ERR_CALL_OPTS_NULL = "callOptionsSupplier must not be null";
    private static final String ERR_NO_QUERY = "No query for QueryExecGrpc";

    private final Channel channel;
    private final Supplier<CallOptions> callOptionsSupplier;
    private boolean parseCheck;
    private Query query;
    private String queryString;
    private final ContextAccumulator contextAcc = ContextAccumulator.newBuilder(ARQ::getContext);
    private final Map<Var, Node> substitutionMap = new HashMap<>();
    private long timeout = -1L;
    private TimeUnit timeoutUnit;

    /**
     * Create a builder targeting {@code channel} with the supplied call options provider and parse-check setting.
     *
     * @param channel the gRPC channel
     * @param callOptionsSupplier supplier of per-call {@link CallOptions}
     * @param parseCheck {@code true} to validate query strings locally when set
     * @throws IllegalArgumentException Thrown if {@code channel} or {@code callOptionsSupplier} is {@code null}
     */
    public QueryExecGrpcBuilder(Channel channel, Supplier<CallOptions> callOptionsSupplier, boolean parseCheck) {
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
    public QueryExecGrpcBuilder query(Query query) {
        Objects.requireNonNull(query);
        this.query = query;
        this.queryString = query.toString();
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public QueryExecGrpcBuilder query(String queryString) {
        Objects.requireNonNull(queryString);
        this.query = parseCheck ? QueryFactory.create(queryString, Syntax.syntaxARQ) : null;
        this.queryString = queryString;
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public QueryExecGrpcBuilder query(String queryString, Syntax syntax) {
        Objects.requireNonNull(queryString);
        this.query = QueryFactory.create(queryString, syntax);
        this.queryString = queryString;
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public QueryExecGrpcBuilder parseCheck(boolean parseCheck) {
        this.parseCheck = parseCheck;
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public QueryExecGrpcBuilder set(Symbol symbol, Object value) {
        contextAcc.set(symbol, value);
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public QueryExecGrpcBuilder set(Symbol symbol, boolean value) {
        contextAcc.set(symbol, value);
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public QueryExecGrpcBuilder context(Context context) {
        if ( context != null ) {
            contextAcc.context(context);
        }
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public Context getContext() {
        return contextAcc.context();
    }

    /** {@inheritDoc} */
    @Override
    public QueryExecGrpcBuilder substitution(Binding binding) {
        Objects.requireNonNull(binding);
        binding.forEach(substitutionMap::put);
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public QueryExecGrpcBuilder substitution(Var var, Node value) {
        Objects.requireNonNull(var);
        Objects.requireNonNull(value);
        substitutionMap.put(var, value);
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public QueryExecGrpcBuilder timeout(long value, TimeUnit timeUnit) {
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
    public QueryExecGrpcBuilder timeout(long timeout) {
        return timeout(timeout, TimeUnit.MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Override
    public QueryExecGrpcBuilder initialTimeout(long timeout, TimeUnit timeUnit) {
        return timeout(timeout, timeUnit);
    }

    /** {@inheritDoc} */
    @Override
    public QueryExecGrpcBuilder overallTimeout(long timeout, TimeUnit timeUnit) {
        return timeout(timeout, timeUnit);
    }

    /** {@inheritDoc} */
    @Override
    public QueryExec build() {
        if ( query == null && queryString == null )
            throw new QueryException(ERR_NO_QUERY);
        Query queryActual = query;
        String queryStringActual = queryString;
        if ( !substitutionMap.isEmpty() ) {
            if ( queryActual == null ) {
                queryActual = QueryFactory.create(queryStringActual, Syntax.syntaxARQ);
            }
            queryActual = QueryTransformOps.replaceVars(queryActual, substitutionMap);
            queryStringActual = queryActual.toString();
        }
        return new QueryExecGrpc(
            channel,
            callOptionsSupplier,
            queryActual,
            queryStringActual,
            contextAcc.context(),
            timeout,
            timeoutUnit);
    }
}
