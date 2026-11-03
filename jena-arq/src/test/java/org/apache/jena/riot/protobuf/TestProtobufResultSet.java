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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.query.ResultSetRewindable;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_DataTuple;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Term;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Term.TermCase;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Var;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_VarTuple;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.exec.RowSet;
import org.apache.jena.sparql.resultset.ResultsCompare;
import org.apache.jena.sparql.sse.Item;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.sparql.sse.builders.BuilderRowSet;

public class TestProtobufResultSet {
    // Only datatypes that are transmitted perfectly.
    static ResultSetRewindable rs0 = make
        ("(resultset (?x ?y)"
         , "   (row (?x _:a) (?y 3))"
         , "   (row (?x 1) (?y 'a'))"
         , "   (row (?y 'y'))"
         , "   (row (?x _:a))"
         , "   (row)"
         , "   (row (?x 2) (?y 10))"
         , "   (row (?x 2) (?y <<(_:a :p :o)>>))"
         , ")"
         );

    static ResultSetRewindable rs1 = make
        ("(resultset (?x ?y)"
         , "   (row (?x 1) (?y 3))"
         , "   (row (?x 1) (?y 'a'))"
         , "   (row (?x 2) (?y <<(:s :p :o)>>))"
         , ")"
         );
    static ResultSetRewindable rs2 = make
        ("(resultset (?x ?y)"
         , "   (row (?x 1) (?y 'a'))"
         , "   (row (?x 2) (?y <<(:s :p :o)>>))"
         , "   (row (?x 1) (?y 3))"
         , ")"
         );

    @Test public void resultSet_01() { test(rs0); }

    @Test public void resultSet_02() {
        ResultSetRewindable r1 = test(rs1);
        // not reordered
        r1.reset();
        rs2.reset();
        assertFalse(ResultsCompare.equalsByTermAndOrder(r1, rs2));
        rs2.reset();
    }

    @Test public void resultSet_03() {
        ResultSetRewindable r2 = test(rs2);
        // not reordered
        r2.reset();
        rs1.reset();
        assertFalse(ResultsCompare.equalsByTermAndOrder(r2, rs1));
        rs1.reset();
    }

    private static ResultSetRewindable test(ResultSetRewindable resultSet) {
        resultSet.reset();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ResultSetFormatter.output(out, resultSet, ResultSetLang.RS_Protobuf);
        resultSet.reset();

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        RowSet rs$ = ProtobufRDF.readRowSet(in);
        ResultSetRewindable resultSet2 = ResultSetFactory.makeRewindable(rs$);
        // Includes bnode labels.
        assertTrue(ResultsCompare.equalsExact(resultSet, resultSet2));
        resultSet.reset();
        resultSet2.reset();
        return resultSet2;
    }

    private static ResultSetRewindable make(String ... strings) {
        String s = StrUtils.strjoinNL(strings);
        Item item = SSE.parse(s);
        ResultSetRewindable rs = ResultSetFactory.makeRewindable(BuilderRowSet.build(item));
        return rs;
    }

    // Wire compatibility with the protobuf library's own delimited read and write,
    // which earlier versions of Jena used directly.

    @Test public void resultSet_wire_write_compat() throws IOException {
        rs0.reset();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ResultSetFormatter.output(out, rs0, ResultSetLang.RS_Protobuf);
        rs0.reset();

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        RDF_VarTuple vars = RDF_VarTuple.parseDelimitedFrom(in);
        assertEquals(List.of("x", "y"), vars.getVarsList().stream().map(RDF_Var::getName).toList());
        int rows = 0;
        while ( true ) {
            RDF_DataTuple row = RDF_DataTuple.parseDelimitedFrom(in);
            if ( row == null )
                break;
            assertEquals(2, row.getRowCount());
            rows++;
        }
        assertEquals(rs0.size(), rows);
    }

    @Test public void resultSet_wire_read_compat() throws IOException {
        rs0.reset();
        List<Var> vars = Var.varList(rs0.getResultVars());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RDF_VarTuple.Builder vrow = RDF_VarTuple.newBuilder();
        vars.forEach(v -> vrow.addVars(RDF_Var.newBuilder().setName(v.getVarName())));
        vrow.build().writeDelimitedTo(out);
        RDF_DataTuple.Builder drow = RDF_DataTuple.newBuilder();
        RDF_Term.Builder term = RDF_Term.newBuilder();
        while ( rs0.hasNext() ) {
            Binding b = rs0.nextBinding();
            drow.clear();
            for ( Var v : vars )
                drow.addRow(ProtobufConvert.toProtobuf(b.get(v), term, false));
            drow.build().writeDelimitedTo(out);
        }
        rs0.reset();

        RowSet rs$ = ProtobufRDF.readRowSet(new ByteArrayInputStream(out.toByteArray()));
        ResultSetRewindable resultSet2 = ResultSetFactory.makeRewindable(rs$);
        assertTrue(ResultsCompare.equalsExact(rs0, resultSet2));
        rs0.reset();
    }

    private static final String DIR = TS_RDFProtobuf.TestingDir;
    private static final List<Var> VARS_X_Y = List.of(Var.alloc("x"), Var.alloc("y"));

    @Test public void resultSet_10() {
        try (InputStream in = IO.openFile(DIR+"/results-1.srj")) {
            ResultSet rs = ResultSetFactory.fromJSON(in);
            test(ResultSetFactory.copyResults(rs));
        } catch (IOException ex) { IO.exception(ex); }
    }

    @Test public void resultSet_messageAdapters_roundTrip() {
        rs0.reset();
        AtomicReference<RDF_VarTuple> headerRef = new AtomicReference<>();
        List<RDF_DataTuple> rows = new ArrayList<>();
        ProtobufRDF.writeRowSet(RowSet.adapt(rs0), headerRef::set, rows::add);
        rs0.reset();

        assertNotNull(headerRef.get());
        assertEquals(VARS_X_Y, ProtobufRDF.decodeVars(headerRef.get()));
        assertEquals(7, rows.size());

        RowSet decoded = ProtobufRDF.readRowSet(headerRef.get(), rows.iterator());
        ResultSetRewindable decodedRewindable = ResultSetFactory.makeRewindable(decoded);
        assertTrue(ResultsCompare.equalsExact(rs0, decodedRewindable));
        rs0.reset();
    }

    @Test public void resultSet_messageAdapters_withValues() {
        rs1.reset();
        AtomicReference<RDF_VarTuple> headerRef = new AtomicReference<>();
        List<RDF_DataTuple> rows = new ArrayList<>();
        ProtobufRDF.writeRowSet(RowSet.adapt(rs1), headerRef::set, rows::add, true);
        rs1.reset();

        assertEquals(TermCase.VALINTEGER, rows.get(0).getRow(0).getTermCase());
        assertEquals(1L, rows.get(0).getRow(0).getValInteger());

        RowSet decoded = ProtobufRDF.readRowSet(ProtobufRDF.decodeVars(headerRef.get()), rows.iterator());
        ResultSetRewindable decodedRewindable = ResultSetFactory.makeRewindable(decoded);
        assertTrue(ResultsCompare.equalsExact(rs1, decodedRewindable));
        rs1.reset();
    }

    @Test public void resultSet_bindingDirectEncodeDecode() {
        RDF_VarTuple varTuple = Binding2Protobuf.encodeVars(VARS_X_Y);
        assertEquals(VARS_X_Y, ProtobufRDF.decodeVars(varTuple));

        List<RDF_DataTuple> captured = new ArrayList<>();
        try ( Binding2Protobuf encoder = new Binding2Protobuf(captured::add, VARS_X_Y, false) ) {
            rs0.reset();
            RowSet.adapt(rs0).forEachRemaining(encoder::output);
            rs0.reset();
        }
        assertEquals(7, captured.size());

        Protobuf2Binding decoder = new Protobuf2Binding(varTuple, captured.iterator());
        List<Binding> bindings = new ArrayList<>();
        decoder.forEachRemaining(bindings::add);
        assertEquals(7, bindings.size());
    }

    @Test public void resultSet_mismatchedRowLengthThrows() {
        RDF_DataTuple badTuple = RDF_DataTuple.newBuilder()
            .addRow(RDF_Term.newBuilder().setValInteger(1L).build())
            .build();
        Protobuf2Binding decoder = new Protobuf2Binding(VARS_X_Y, List.of(badTuple).iterator());
        assertThrows(RiotProtobufException.class, decoder::next);
    }

    @Test public void resultSet_validation() {
        assertThrows(IllegalArgumentException.class, () -> Binding2Protobuf.encodeVars(null));
        assertThrows(IllegalArgumentException.class, () -> new Binding2Protobuf(r -> {}, null, false));
        assertThrows(IllegalArgumentException.class, () -> ProtobufRDF.decodeVars(null));
        assertThrows(IllegalArgumentException.class, () -> Protobuf2Binding.decodeVars(null));
        assertThrows(IllegalArgumentException.class,
            () -> ProtobufRDF.writeRowSet(null, h -> {}, r -> {}));
        assertThrows(IllegalArgumentException.class,
            () -> ProtobufRDF.writeRowSet(RowSet.adapt(rs1), null, r -> {}));
        assertThrows(IllegalArgumentException.class,
            () -> ProtobufRDF.writeRowSet(RowSet.adapt(rs1), h -> {}, null));
        assertThrows(IllegalArgumentException.class,
            () -> ProtobufRDF.readRowSet((RDF_VarTuple) null, List.<RDF_DataTuple>of().iterator()));
        assertThrows(IllegalArgumentException.class,
            () -> ProtobufRDF.readRowSet(RDF_VarTuple.getDefaultInstance(), null));
    }
}
