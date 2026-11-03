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

import java.io.OutputStream;

import com.google.protobuf.CodedOutputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Consumer;

import org.apache.jena.graph.Node;
import org.apache.jena.riot.protobuf.wire.PB_RDF.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;

/** Converted from Bindings to SPARQL result set encoded in Protobuf */
public class Binding2Protobuf implements AutoCloseable {
    private static final String ERR_OUT_NULL = "out must not be null";
    private static final String ERR_VARS_NULL = "vars must not be null";
    private static final String ERR_HEADER_CONSUMER_NULL = "headerConsumer must not be null";
    private static final String ERR_ROW_CONSUMER_NULL = "rowConsumer must not be null";
    private static final String ERR_BINDING_NULL = "binding must not be null";

    private final RDF_DataTuple.Builder row = RDF_DataTuple.newBuilder();
    private final RDF_Term.Builder term = RDF_Term.newBuilder();
    private final Collection<Var> vars;
    private final Consumer<RDF_DataTuple> rowConsumer;
    private final Runnable onClose;
    private final boolean encodeValues;

    /**
     * Create an encoder that writes the {@link RDF_VarTuple} header and subsequent {@link RDF_DataTuple}
     * rows to a length-delimited {@link OutputStream}.
     *
     * @param out destination output stream
     * @param vars result set variables
     * @param encodeValues {@code true} to encode numeric literals by value
     * @throws IllegalArgumentException Thrown if {@code out} or {@code vars} is {@code null}
     */
    public Binding2Protobuf(OutputStream out, Collection<Var> vars, boolean encodeValues) {
        if ( out == null )
            throw new IllegalArgumentException(ERR_OUT_NULL);
        if ( vars == null )
            throw new IllegalArgumentException(ERR_VARS_NULL);
        CodedOutputStream codedOutput = PBufRDF.delimitedOutput(out);
        this.rowConsumer = drow -> PBufRDF.writeDelimited(drow, codedOutput);
        this.onClose = () -> PBufRDF.flush(codedOutput);
        this.vars = vars;
        this.encodeValues = encodeValues;
        PBufRDF.writeDelimited(encodeVars(vars), codedOutput);
    }

    /**
     * Create an encoder that emits each encoded {@link RDF_DataTuple} row to {@code rowConsumer}
     * without emitting a header message.
     *
     * @param rowConsumer consumer receiving each encoded {@link RDF_DataTuple}
     * @param vars result set variables
     * @param encodeValues {@code true} to encode numeric literals by value
     * @throws IllegalArgumentException Thrown if {@code rowConsumer} or {@code vars} is {@code null}
     */
    public Binding2Protobuf(Consumer<RDF_DataTuple> rowConsumer, Collection<Var> vars, boolean encodeValues) {
        if ( rowConsumer == null )
            throw new IllegalArgumentException(ERR_ROW_CONSUMER_NULL);
        if ( vars == null )
            throw new IllegalArgumentException(ERR_VARS_NULL);
        this.rowConsumer = rowConsumer;
        this.onClose = () -> {};
        this.vars = vars;
        this.encodeValues = encodeValues;
    }

    /**
     * Create an encoder that immediately emits the {@link RDF_VarTuple} header to {@code headerConsumer}
     * and emits each subsequent {@link RDF_DataTuple} row to {@code rowConsumer}.
     *
     * @param headerConsumer consumer receiving the initial {@link RDF_VarTuple} variable header
     * @param rowConsumer consumer receiving each encoded {@link RDF_DataTuple}
     * @param vars result set variables
     * @param encodeValues {@code true} to encode numeric literals by value
     * @throws IllegalArgumentException Thrown if {@code headerConsumer}, {@code rowConsumer}, or {@code vars} is {@code null}
     */
    public Binding2Protobuf(Consumer<RDF_VarTuple> headerConsumer, Consumer<RDF_DataTuple> rowConsumer, Collection<Var> vars, boolean encodeValues) {
        if ( headerConsumer == null )
            throw new IllegalArgumentException(ERR_HEADER_CONSUMER_NULL);
        if ( rowConsumer == null )
            throw new IllegalArgumentException(ERR_ROW_CONSUMER_NULL);
        if ( vars == null )
            throw new IllegalArgumentException(ERR_VARS_NULL);
        this.rowConsumer = rowConsumer;
        this.onClose = () -> {};
        this.vars = vars;
        this.encodeValues = encodeValues;
        headerConsumer.accept(encodeVars(vars));
    }

    /**
     * Encode a collection of SPARQL result variables into an {@link RDF_VarTuple} message.
     *
     * @param vars the result variables in column order
     * @return the encoded {@link RDF_VarTuple}
     * @throws IllegalArgumentException Thrown if {@code vars} is {@code null}
     */
    public static RDF_VarTuple encodeVars(Collection<Var> vars) {
        if ( vars == null )
            throw new IllegalArgumentException(ERR_VARS_NULL);
        RDF_VarTuple.Builder vrow = RDF_VarTuple.newBuilder();
        RDF_Var.Builder var = RDF_Var.newBuilder();
        vars.forEach(v -> {
            var.clear();
            var.setName(v.getVarName());
            vrow.addVars(var);
        });
        return vrow.build();
    }

    /**
     * Encode a single {@link Binding} into an {@link RDF_DataTuple} message using the configured variable list.
     *
     * @param binding the binding to encode
     * @return the encoded {@link RDF_DataTuple}
     * @throws IllegalArgumentException Thrown if {@code binding} is {@code null}
     */
    public RDF_DataTuple encode(Binding binding) {
        if ( binding == null )
            throw new IllegalArgumentException(ERR_BINDING_NULL);
        row.clear();
        Iterator<Var> vIter = vars.iterator();
        vIter.forEachRemaining(v -> {
            term.clear();
            Node n = binding.get(v);
            RDF_Term rt = ProtobufConvert.toProtobuf(n, term, encodeValues);
            row.addRow(rt);
        });
        return row.build();
    }

    /**
     * Encode {@code binding} as an {@link RDF_DataTuple} and send it to the configured output consumer.
     *
     * @param binding the binding to output
     * @throws IllegalArgumentException Thrown if {@code binding} is {@code null}
     */
    public void output(Binding binding) {
        rowConsumer.accept(encode(binding));
    }

    /**
     * Flush any buffered output. When constructed over an {@link OutputStream},
     * that stream is flushed but not closed.
     */
    @Override
    public void close() {
        onClose.run();
    }
}
