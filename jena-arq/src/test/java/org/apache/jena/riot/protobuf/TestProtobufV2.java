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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.protobuf.wire.PB_RDF.*;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.graph.GraphFactory;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.sparql.util.IsoMatcher;
import org.junit.jupiter.api.Test;

/** The version 2 encoding: stream header, term and datatype tables, repeats. */
public class TestProtobufV2 {

    static String gs = StrUtils.strjoinNL(
        "(graph",
        "  (:s1 :p :o)",
        "  (:s1 :p 'text')",
        "  (:s1 :q 'label'@en)",
        "  (:s1 :q 'label'@en--ltr)",
        "  (:s1 :r 123)",
        "  (:s1 :r '456'^^xsd:long)",
        "  (:s1 :r 45.6)",
        "  (:s1 :r 4.5e6)",
        "  (:s1 :r '2026-11-03'^^xsd:date)",
        "  (:s2 :p _:b)",
        "  (_:b :p :s1)",
        "  (:s2 :r '2026-11-04'^^xsd:date)",
        "  (:o :p :s2)",
        ")");

    static Graph graph = SSE.parseGraph(gs);

    static String dgs = StrUtils.strjoinNL(
        "(dataset",
        "  (graph (:s1 :p _:a) (:s2 :p _:a))",
        "  (graph :g1 (:s1 :p _:a) (:s1 :q :o) (:s2 :p 1))",
        "  (graph :g2 (:s2 :p _:a))",
        ")");

    static DatasetGraph datasetGraph = SSE.parseDatasetGraph(dgs);

    // ---- Round trips through the registered formats.

    @Test public void graph_v2() {
        Graph g2 = writeRead(graph, RDFFormat.RDF_PROTO_V2);
        assertTrue(IsoMatcher.isomorphic(graph, g2));
    }

    @Test public void graph_v2_values() {
        Graph g2 = writeRead(graph, RDFFormat.RDF_PROTO_V2_VALUES);
        // Value encoding turns xsd:long into xsd:integer, so compare on what is preserved.
        Node s1 = SSE.parseNode(":s1");
        Node r = SSE.parseNode(":r");
        assertEquals(graph.find(s1, r, null).toList().size(), g2.find(s1, r, null).toList().size());
        assertTrue(g2.contains(s1, r, NodeFactory.createLiteralDT("123", XSDDatatype.XSDinteger)));
        assertTrue(g2.contains(s1, r, NodeFactory.createLiteralDT("456", XSDDatatype.XSDinteger)));
        assertTrue(g2.contains(s1, r, NodeFactory.createLiteralDT("45.6", XSDDatatype.XSDdecimal)));
        assertTrue(g2.contains(s1, r, NodeFactory.createLiteralDT("2026-11-03", XSDDatatype.XSDdate)));
    }

    @Test public void graph_v1_still_read() {
        Graph g2 = writeRead(graph, RDFFormat.RDF_PROTO);
        assertTrue(IsoMatcher.isomorphic(graph, g2));
    }

    @Test public void dataset_v2() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RDFDataMgr.write(out, datasetGraph, RDFFormat.RDF_PROTO_V2);
        DatasetGraph dsg2 = DatasetGraphFactory.create();
        RDFDataMgr.read(dsg2, new ByteArrayInputStream(out.toByteArray()), Lang.RDFPROTO);
        assertTrue(IsoMatcher.isomorphic(datasetGraph, dsg2));
    }

    @Test public void dataset_v2_values() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RDFDataMgr.write(out, datasetGraph, RDFFormat.RDF_PROTO_V2_VALUES);
        DatasetGraph dsg2 = DatasetGraphFactory.create();
        RDFDataMgr.read(dsg2, new ByteArrayInputStream(out.toByteArray()), Lang.RDFPROTO);
        assertTrue(IsoMatcher.isomorphic(datasetGraph, dsg2));
    }

    @Test public void empty_stream_v2() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StreamRDF stream = ProtobufRDF.streamToOutputStream(out, false, true);
        stream.start();
        stream.finish();
        List<RDF_StreamRow> rows = rows(out.toByteArray());
        assertEquals(1, rows.size());
        assertEquals(RDF_StreamRow.RowCase.HEADER, rows.get(0).getRowCase());
        Graph g2 = GraphFactory.createDefaultGraph();
        ProtobufRDF.inputStreamToStreamRDF(new ByteArrayInputStream(out.toByteArray()), StreamRDFLib.graph(g2));
        assertTrue(g2.isEmpty());
    }

    // ---- What goes on the wire.

    @Test public void wire_header_references_repeats() {
        List<Triple> triples = List.of(
            SSE.parseTriple("(:s :p :o1)"),
            SSE.parseTriple("(:s :p :o2)"),
            SSE.parseTriple("(:s :q :o1)"),
            SSE.parseTriple("(:o1 :q :s)"));
        List<RDF_StreamRow> rows = rows(writeV2(triples, false));
        assertEquals(5, rows.size());

        RDF_Header header = rows.get(0).getHeader();
        assertEquals(2, header.getVersion());
        assertEquals(ProtobufEncoderV2.DFT_TERM_TABLE_SIZE, header.getTermTableSize());
        assertEquals(ProtobufEncoderV2.DFT_DATATYPE_TABLE_SIZE, header.getDatatypeTableSize());

        // First row: everything in full. Table slots: :s=0, :p=1, :o1=2.
        RDF_Triple t1 = rows.get(1).getTriple();
        assertEquals(RDF_Term.TermCase.IRI, t1.getS().getTermCase());
        assertEquals(RDF_Term.TermCase.IRI, t1.getP().getTermCase());
        assertEquals(RDF_Term.TermCase.IRI, t1.getO().getTermCase());

        // Same subject and predicate as the row before. :o2=3.
        RDF_Triple t2 = rows.get(2).getTriple();
        assertEquals(RDF_Term.TermCase.REPEAT, t2.getS().getTermCase());
        assertEquals(RDF_Term.TermCase.REPEAT, t2.getP().getTermCase());
        assertEquals(RDF_Term.TermCase.IRI, t2.getO().getTermCase());

        // :q=4 is new; :o1 was seen before and is in slot 2.
        RDF_Triple t3 = rows.get(3).getTriple();
        assertEquals(RDF_Term.TermCase.REPEAT, t3.getS().getTermCase());
        assertEquals(RDF_Term.TermCase.IRI, t3.getP().getTermCase());
        assertEquals(RDF_Term.TermCase.TERMREF, t3.getO().getTermCase());
        assertEquals(2, t3.getO().getTermRef());

        // Terms seen before in other positions are references, not repeats.
        RDF_Triple t4 = rows.get(4).getTriple();
        assertEquals(RDF_Term.TermCase.TERMREF, t4.getS().getTermCase());
        assertEquals(2, t4.getS().getTermRef());
        assertEquals(RDF_Term.TermCase.REPEAT, t4.getP().getTermCase());
        assertEquals(RDF_Term.TermCase.TERMREF, t4.getO().getTermCase());
        assertEquals(0, t4.getO().getTermRef());
    }

    @Test public void wire_datatype_reference() {
        List<Triple> triples = List.of(
            SSE.parseTriple("(:s :p '2026-11-03'^^xsd:date)"),
            SSE.parseTriple("(:s :p '2026-11-04'^^xsd:date)"),
            SSE.parseTriple("(:s :p '1'^^xsd:long)"),
            SSE.parseTriple("(:s :p '2026-11-05'^^xsd:date)"));
        List<RDF_StreamRow> rows = rows(writeV2(triples, false));
        RDF_Literal lit1 = rows.get(1).getTriple().getO().getLiteral();
        assertEquals(RDF_Literal.LiteralKindCase.DATATYPE, lit1.getLiteralKindCase());
        assertEquals(XSDDatatype.XSDdate.getURI(), lit1.getDatatype());
        RDF_Literal lit2 = rows.get(2).getTriple().getO().getLiteral();
        assertEquals(RDF_Literal.LiteralKindCase.DATATYPEREF, lit2.getLiteralKindCase());
        assertEquals(0, lit2.getDatatypeRef());
        RDF_Literal lit3 = rows.get(3).getTriple().getO().getLiteral();
        assertEquals(RDF_Literal.LiteralKindCase.DATATYPE, lit3.getLiteralKindCase());
        RDF_Literal lit4 = rows.get(4).getTriple().getO().getLiteral();
        assertEquals(RDF_Literal.LiteralKindCase.DATATYPEREF, lit4.getLiteralKindCase());
        assertEquals(0, lit4.getDatatypeRef());
    }

    @Test public void wire_quads_graph_repeat() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StreamRDF stream = ProtobufRDF.streamToOutputStream(out, false, true);
        stream.start();
        stream.quad(SSE.parseQuad("(:g :s :p :o1)"));
        stream.quad(SSE.parseQuad("(:g :s :p :o2)"));
        stream.quad(SSE.parseQuad("(_ :s :p :o1)"));
        stream.triple(SSE.parseTriple("(:s :p :o1)"));
        stream.finish();
        List<RDF_StreamRow> rows = rows(out.toByteArray());
        assertEquals(5, rows.size());
        RDF_Quad q2 = rows.get(2).getQuad();
        assertEquals(RDF_Term.TermCase.REPEAT, q2.getG().getTermCase());
        assertEquals(RDF_Term.TermCase.REPEAT, q2.getS().getTermCase());
        // The default graph node is a term like any other: in full the first time.
        RDF_Quad q3 = rows.get(3).getQuad();
        assertEquals(RDF_Term.TermCase.IRI, q3.getG().getTermCase());
        assertEquals(RDF_Term.TermCase.REPEAT, q3.getS().getTermCase());
        assertEquals(RDF_Term.TermCase.TERMREF, q3.getO().getTermCase());
        // A triple after quads: subject, predicate and object repeat.
        RDF_Triple t4 = rows.get(4).getTriple();
        assertEquals(RDF_Term.TermCase.REPEAT, t4.getS().getTermCase());
        assertEquals(RDF_Term.TermCase.REPEAT, t4.getP().getTermCase());
        assertEquals(RDF_Term.TermCase.REPEAT, t4.getO().getTermCase());
    }

    @Test public void wire_triple_term_uses_tables() {
        Triple inner = SSE.parseTriple("(:s :p :o)");
        Node tripleTerm = NodeFactory.createTripleTerm(inner);
        List<Triple> triples = List.of(
            Triple.create(tripleTerm, SSE.parseNode(":q"), SSE.parseNode(":o")),
            inner);
        byte[] bytes = writeV2(triples, false);
        List<RDF_StreamRow> rows = rows(bytes);
        // Inside the triple term: :s=0, :p=1, :o=2 in full; then :q=3; then :o as a reference.
        RDF_Triple t1 = rows.get(1).getTriple();
        assertEquals(RDF_Term.TermCase.TRIPLETERM, t1.getS().getTermCase());
        assertEquals(RDF_Term.TermCase.TERMREF, t1.getO().getTermCase());
        assertEquals(2, t1.getO().getTermRef());
        // The same triple as a row: subject and predicate are references, the object repeats.
        RDF_Triple t2 = rows.get(2).getTriple();
        assertEquals(RDF_Term.TermCase.TERMREF, t2.getS().getTermCase());
        assertEquals(0, t2.getS().getTermRef());
        assertEquals(RDF_Term.TermCase.TERMREF, t2.getP().getTermCase());
        assertEquals(1, t2.getP().getTermRef());
        assertEquals(RDF_Term.TermCase.REPEAT, t2.getO().getTermCase());
        assertEquals(triples, readV2(bytes));
    }

    // ---- Tables that wrap round.

    @Test public void table_wrap_round() {
        List<Triple> triples = new ArrayList<>();
        for ( int i = 0; i < 100; i++ ) {
            Node s = NodeFactory.createURI("http://example/s"+(i%7));
            Node p = NodeFactory.createURI("http://example/p"+(i%3));
            Node o = ( i%2 == 0 )
                ? NodeFactory.createURI("http://example/o"+(i%11))
                : NodeFactory.createLiteralDT(Integer.toString(i), ( i%4 == 1 ) ? XSDDatatype.XSDlong : XSDDatatype.XSDshort);
            triples.add(Triple.create(s, p, o));
        }
        // Tables far smaller than the number of distinct terms and datatypes.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StreamRDF stream = StreamRDF2Protobuf.createDelimited(out, false, 5, 1);
        stream.start();
        triples.forEach(stream::triple);
        stream.finish();
        List<RDF_StreamRow> rows = rows(out.toByteArray());
        assertEquals(5, rows.get(0).getHeader().getTermTableSize());
        assertEquals(1, rows.get(0).getHeader().getDatatypeTableSize());
        assertEquals(triples, readV2(out.toByteArray()));
    }

    // ---- Streams and errors.

    @Test public void concatenated_streams() {
        List<Triple> triples1 = List.of(SSE.parseTriple("(:s :p :o1)"), SSE.parseTriple("(:s :p :o2)"));
        List<Triple> triples2 = List.of(SSE.parseTriple("(:s :p :o3)"), SSE.parseTriple("(:s :p :o1)"));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(writeV2(triples1, false));
        out.writeBytes(writeV2(triples2, false));
        List<Triple> expected = new ArrayList<>(triples1);
        expected.addAll(triples2);
        assertEquals(expected, readV2(out.toByteArray()));
    }

    @Test public void reference_without_header() throws IOException {
        RDF_Term ref = RDF_Term.newBuilder().setTermRef(0).build();
        RDF_Triple triple = RDF_Triple.newBuilder().setS(ref).setP(ref).setO(ref).build();
        RDF_StreamRow row = RDF_StreamRow.newBuilder().setTriple(triple).build();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CodedOutputStream coded = CodedOutputStream.newInstance(out);
        PBufRDF.writeDelimited(row, coded);
        coded.flush();
        Graph g2 = GraphFactory.createDefaultGraph();
        assertThrows(RiotProtobufException.class,
                     () -> ProtobufRDF.inputStreamToStreamRDF(new ByteArrayInputStream(out.toByteArray()), StreamRDFLib.graph(g2)));
    }

    @Test public void repeat_without_previous() throws IOException {
        RDF_Header header = RDF_Header.newBuilder().setVersion(2).setTermTableSize(10).setDatatypeTableSize(10).build();
        RDF_Term repeat = RDF_Term.newBuilder().setRepeat(RDF_REPEAT.getDefaultInstance()).build();
        RDF_Triple triple = RDF_Triple.newBuilder().setS(repeat).setP(repeat).setO(repeat).build();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CodedOutputStream coded = CodedOutputStream.newInstance(out);
        PBufRDF.writeDelimited(RDF_StreamRow.newBuilder().setHeader(header).build(), coded);
        PBufRDF.writeDelimited(RDF_StreamRow.newBuilder().setTriple(triple).build(), coded);
        coded.flush();
        Graph g2 = GraphFactory.createDefaultGraph();
        assertThrows(RiotProtobufException.class,
                     () -> ProtobufRDF.inputStreamToStreamRDF(new ByteArrayInputStream(out.toByteArray()), StreamRDFLib.graph(g2)));
    }

    @Test public void bad_header() throws IOException {
        RDF_Header header = RDF_Header.newBuilder().setVersion(3).setTermTableSize(10).setDatatypeTableSize(10).build();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CodedOutputStream coded = CodedOutputStream.newInstance(out);
        PBufRDF.writeDelimited(RDF_StreamRow.newBuilder().setHeader(header).build(), coded);
        coded.flush();
        Graph g2 = GraphFactory.createDefaultGraph();
        assertThrows(RiotProtobufException.class,
                     () -> ProtobufRDF.inputStreamToStreamRDF(new ByteArrayInputStream(out.toByteArray()), StreamRDFLib.graph(g2)));
    }

    // ---- Helpers

    @Test public void frames_v2_round_trip() {
        framesRoundTrip(true, 7);
    }

    @Test public void frames_v1_round_trip() {
        framesRoundTrip(false, 7);
    }

    @Test public void frames_one_row_each() {
        framesRoundTrip(true, 1);
    }

    @Test public void frames_parse_as_messages() throws Exception {
        List<Triple> triples = sampleTriples();
        List<java.nio.ByteBuffer> frames = new ArrayList<>();
        StreamRDF writer = ProtobufRDF.streamToFrames(frames::add, 3, false, true);
        writer.start();
        writer.prefix("ex", "http://example/");
        triples.forEach(writer::triple);
        writer.finish();
        // Each frame is a well-formed RDF_Stream message with the expected rows.
        int rows = 0;
        boolean headerFirst = false;
        for ( int i = 0; i < frames.size(); i++ ) {
            org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Stream msg =
                org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Stream.parseFrom(frames.get(i));
            if ( i == 0 )
                headerFirst = msg.getRow(0).hasHeader();
            rows += msg.getRowCount();
        }
        assertTrue(headerFirst);
        assertEquals(1 + 1 + triples.size(), rows);  // header, prefix, triples
        // The generated-message decoder reads the same frames.
        List<Triple> back = new ArrayList<>();
        StreamRDF collector = new org.apache.jena.riot.system.StreamRDFBase() {
            @Override public void triple(Triple triple) { back.add(triple); }
        };
        java.util.function.Consumer<org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Stream> decoder =
            ProtobufRDF.batchConsumerToStreamRDF(collector);
        for ( java.nio.ByteBuffer frame : frames )
            decoder.accept(org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Stream.parseFrom(frame));
        assertEquals(triples, back);
    }

    @Test public void frames_empty_stream() {
        List<java.nio.ByteBuffer> frames = new ArrayList<>();
        StreamRDF writer = ProtobufRDF.streamToFrames(frames::add, 10, false, true);
        writer.start();
        writer.finish();
        // Version 2: the header row alone. Version 1: nothing at all.
        assertEquals(1, frames.size());
        StreamRDF writer1 = ProtobufRDF.streamToFrames(frames::add, 10, false, false);
        writer1.start();
        writer1.finish();
        assertEquals(1, frames.size());
        List<Triple> back = new ArrayList<>();
        StreamRDF collector = new org.apache.jena.riot.system.StreamRDFBase() {
            @Override public void triple(Triple triple) { back.add(triple); }
        };
        java.util.function.Consumer<java.nio.ByteBuffer> decoder = ProtobufRDF.framesToStreamRDF(collector);
        frames.forEach(decoder);
        assertTrue(back.isEmpty());
    }

    private static void framesRoundTrip(boolean version2, int batchSize) {
        List<Triple> triples = sampleTriples();
        List<java.nio.ByteBuffer> frames = new ArrayList<>();
        StreamRDF writer = ProtobufRDF.streamToFrames(frames::add, batchSize, false, version2);
        writer.start();
        triples.forEach(writer::triple);
        writer.finish();
        assertEquals((triples.size() + (version2 ? 1 : 0) + batchSize - 1) / batchSize, frames.size());
        List<Triple> back = new ArrayList<>();
        StreamRDF collector = new org.apache.jena.riot.system.StreamRDFBase() {
            @Override public void triple(Triple triple) { back.add(triple); }
        };
        java.util.function.Consumer<java.nio.ByteBuffer> decoder = ProtobufRDF.framesToStreamRDF(collector);
        frames.forEach(decoder);
        assertEquals(triples, back);
    }

    private static List<Triple> sampleTriples() {
        List<Triple> triples = new ArrayList<>();
        for ( int i = 0; i < 20; i++ ) {
            Node s = NodeFactory.createURI("http://example/s" + (i / 4));
            Node p = NodeFactory.createURI("http://example/p" + (i % 3));
            Node o = switch ( i % 4 ) {
                case 0 -> NodeFactory.createURI("http://example/o" + i);
                case 1 -> NodeFactory.createLiteralString("text " + i);
                case 2 -> NodeFactory.createLiteralDT(Integer.toString(i), org.apache.jena.datatypes.xsd.XSDDatatype.XSDinteger);
                default -> NodeFactory.createBlankNode("b" + i);
            };
            triples.add(Triple.create(s, p, o));
        }
        return triples;
    }

    private static Graph writeRead(Graph g, RDFFormat format) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RDFDataMgr.write(out, g, format);
        Graph g2 = GraphFactory.createDefaultGraph();
        RDFDataMgr.read(g2, new ByteArrayInputStream(out.toByteArray()), Lang.RDFPROTO);
        return g2;
    }

    private static byte[] writeV2(List<Triple> triples, boolean withValues) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StreamRDF stream = ProtobufRDF.streamToOutputStream(out, withValues, true);
        stream.start();
        triples.forEach(stream::triple);
        stream.finish();
        return out.toByteArray();
    }

    private static List<Triple> readV2(byte[] bytes) {
        List<Triple> triples = new ArrayList<>();
        StreamRDF collector = new org.apache.jena.riot.system.StreamRDFBase() {
            @Override public void triple(Triple triple) { triples.add(triple); }
        };
        ProtobufRDF.inputStreamToStreamRDF(new ByteArrayInputStream(bytes), collector);
        return triples;
    }

    private static List<RDF_StreamRow> rows(byte[] bytes) {
        List<RDF_StreamRow> rows = new ArrayList<>();
        try {
            CodedInputStream in = PBufRDF.delimitedInput(new ByteArrayInputStream(bytes));
            while (true) {
                RDF_StreamRow row = PBufRDF.readDelimited(in, RDF_StreamRow.parser());
                if ( row == null )
                    break;
                rows.add(row);
            }
        } catch (IOException ex) { throw new RuntimeException(ex); }
        return rows;
    }
}
