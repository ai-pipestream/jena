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

package org.apache.jena.riot.protobuf;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.iterator.IteratorSlotted;
import org.apache.jena.atlas.lib.Cache;
import org.apache.jena.graph.Node;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_DataTuple;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Term;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_VarTuple;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;

/** Convert from SPARQL result set encoded in Protobuf to Bindings */
public class Protobuf2Binding extends IteratorSlotted<Binding> implements Iterator<Binding> {

    private List<Var> vars = new ArrayList<>();
    private List<String> varNames = new ArrayList<>();
    private final InputStream input;
    private final Iterator<RDF_DataTuple> dataTuples;
    private final Cache<String, Node> uriCache;
    private final BindingBuilder b = Binding.builder();

    /**
     * Create a streaming {@link Binding} iterator reading length-delimited {@link RDF_VarTuple}
     * and {@link RDF_DataTuple} messages from an {@link InputStream}.
     *
     * @param input the source input stream
     * @throws IllegalArgumentException Thrown if {@code input} is {@code null}
     */
    public Protobuf2Binding(InputStream input) {
        if ( input == null )
            throw new IllegalArgumentException("input must not be null");
        this.input = input;
        this.dataTuples = null;
        this.uriCache = null;
        readVars();
    }

    /**
     * Create a streaming {@link Binding} iterator from a decoded {@link RDF_VarTuple} header
     * and an iterator of {@link RDF_DataTuple} row messages.
     *
     * @param varTuple the result variable header message
     * @param dataTuples iterator supplying {@link RDF_DataTuple} row messages
     * @throws IllegalArgumentException Thrown if {@code varTuple} or {@code dataTuples} is {@code null}
     */
    public Protobuf2Binding(RDF_VarTuple varTuple, Iterator<RDF_DataTuple> dataTuples) {
        this(decodeVars(varTuple), dataTuples, null);
    }

    /**
     * Create a streaming {@link Binding} iterator from an explicit variable list
     * and an iterator of {@link RDF_DataTuple} row messages.
     *
     * @param vars the result variables in column order
     * @param dataTuples iterator supplying {@link RDF_DataTuple} row messages
     * @throws IllegalArgumentException Thrown if {@code vars} or {@code dataTuples} is {@code null}
     */
    public Protobuf2Binding(List<Var> vars, Iterator<RDF_DataTuple> dataTuples) {
        this(vars, dataTuples, null);
    }

    /**
     * Create a streaming {@link Binding} iterator from an explicit variable list,
     * an iterator of {@link RDF_DataTuple} row messages, and an optional URI node cache.
     *
     * @param vars the result variables in column order
     * @param dataTuples iterator supplying {@link RDF_DataTuple} row messages
     * @param uriCache optional cache for interning URI nodes, or {@code null} to disable caching
     * @throws IllegalArgumentException Thrown if {@code vars} or {@code dataTuples} is {@code null}
     */
    public Protobuf2Binding(List<Var> vars, Iterator<RDF_DataTuple> dataTuples, Cache<String, Node> uriCache) {
        if ( vars == null )
            throw new IllegalArgumentException("vars must not be null");
        if ( dataTuples == null )
            throw new IllegalArgumentException("dataTuples must not be null");
        this.input = null;
        this.dataTuples = dataTuples;
        this.uriCache = uriCache;
        this.vars = List.copyOf(vars);
        for ( Var v : this.vars ) {
            this.varNames.add(v.getVarName());
        }
    }

    /**
     * Decode an {@link RDF_VarTuple} message into an ordered list of {@link Var} instances.
     *
     * @param varTuple the variable header message
     * @return an ordered list of {@link Var}
     * @throws IllegalArgumentException Thrown if {@code varTuple} is {@code null}
     */
    public static List<Var> decodeVars(RDF_VarTuple varTuple) {
        if ( varTuple == null )
            throw new IllegalArgumentException("varTuple must not be null");
        List<String> names = new ArrayList<>(varTuple.getVarsCount());
        varTuple.getVarsList().forEach(rv -> names.add(rv.getName()));
        return Var.varList(names);
    }

    private void readVars() {
        try {
            RDF_VarTuple vrow = RDF_VarTuple.parseDelimitedFrom(input);
            if ( vrow != null )
                vrow.getVarsList().forEach(rv->varNames.add(rv.getName()));
            vars = Var.varList(varNames);
        } catch (IOException ex) { IO.exception(ex); }
    }

    /**
     * Return the ordered list of result variables.
     *
     * @return the result variables
     */
    public List<Var> getVars()              { return vars; }

    /**
     * Return the ordered list of result variable names.
     *
     * @return the result variable names
     */
    public List<String> getVarNames()       { return varNames; }

    /**
     * Decode a single {@link RDF_DataTuple} row message into a {@link Binding}.
     *
     * @param dataTuple the row message to decode
     * @return the decoded {@link Binding}
     * @throws IllegalArgumentException Thrown if {@code dataTuple} is {@code null}
     * @throws RiotProtobufException Thrown if the row width does not match the number of result variables
     */
    public Binding decode(RDF_DataTuple dataTuple) {
        if ( dataTuple == null )
            throw new IllegalArgumentException("dataTuple must not be null");
        b.reset();
        List<RDF_Term> row = dataTuple.getRowList();
        if ( row.size() != vars.size() )
            throw new RiotProtobufException(String.format("Vars %d : Row length : %d", vars.size(), row.size()));
        for ( int i = 0;  i < vars.size(); i++ ) {
            // Old school
            Var v = vars.get(i);
            RDF_Term rt = row.get(i);
            if ( rt.hasUndefined() )
                continue;
            Node n = ProtobufConvert.convert(uriCache, rt);
            b.add(v, n);
        }
        return b.build();
    }

    /** {@inheritDoc} */
    @Override
    protected Binding moveToNext() {
        if ( dataTuples != null ) {
            if ( !dataTuples.hasNext() )
                return null;
            return decode(dataTuples.next());
        }
        try {
            RDF_DataTuple dataTuple = RDF_DataTuple.parseDelimitedFrom(input);
            if ( dataTuple == null )
                return null;
            return decode(dataTuple);
        } catch (IOException ex) { IO.exception(ex); return null; }
    }

    /** {@inheritDoc} */
    @Override
    protected boolean hasMore() {
        return dataTuples == null || dataTuples.hasNext();
    }
}
