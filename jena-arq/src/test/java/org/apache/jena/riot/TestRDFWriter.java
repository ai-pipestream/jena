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

package org.apache.jena.riot;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.CharArrayWriter;
import java.io.Writer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.jena.atlas.io.AWriter;
import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.lang.LabelToNode;
import org.apache.jena.riot.out.NodeFormatter;
import org.apache.jena.riot.out.NodeFormatterNT;
import org.apache.jena.riot.out.NodeToLabel;
import org.apache.jena.riot.system.StreamRDFWriter;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.graph.GraphFactory;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.sparql.util.Context;

public class TestRDFWriter {
    private static Graph graph = SSE.parseGraph("(graph (:s :p :o))");
    private static final String BNODE_LABEL = "students";
    private static final String CUSTOM_BNODE_PREFIX = "_:custom_";
    private static final Node BNODE = NodeFactory.createBlankNode(BNODE_LABEL);
    private static final Node PRED = NodeFactory.createURI("http://example/p");
    private static final Node OBJ = NodeFactory.createURI("http://example/o");
    private static final Node GRAPH_NAME = NodeFactory.createURI("http://example/g");

    @Test public void rdfwriter_create() {
        RDFWriter.source(graph).build();
    }

    @Test public void rdfwriter_string_output_1() {
        String s =
            RDFWriter.create()
                .source(graph)
                .lang(Lang.NT)
                .asString();
        assertTrue(s.contains("example/s"));
    }

    @Test public void rdfwriter_string_output_2() {
        String s =
            RDFWriter.create()
                .source(graph)
                .lang(Lang.NT)
                .toString();
        assertTrue(s.contains("example/s"));
    }


    @Test
    public void rdfwriter_no_source() {
        assertThrows(RiotException.class,()->
            // No source
            RDFWriter.create().build()
        );
    }

    @Test
    public void rdfwriter_no_syntax() {
        assertThrows(RiotException.class,()->
            RDFWriter.create()
                // No syntax
                .source(graph)
                .asString()
            );
    }

    @Test public void rdfwriter_output_1() {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        RDFWriter.create()
            .source(graph)
            .lang(Lang.NT)
            .output(bout);
        String s = StrUtils.fromUTF8bytes(bout.toByteArray());
        assertTrue(s.contains("example/s"));
    }

    @SuppressWarnings("deprecation")
    @Test public void rdfwriter_output_2() {
        Writer w = new CharArrayWriter();
        RDFWriter.create()
            .source(graph)
            .lang(Lang.NT)
            .build()
            .output(w);
        String s = w.toString();
        assertTrue(s.contains("example/s"));
    }

    private static Graph createBNodeGraph() {
        Graph g = GraphFactory.createDefaultGraph();
        g.add(Triple.create(BNODE, PRED, OBJ));
        return g;
    }

    private static DatasetGraph createBNodeDataset() {
        DatasetGraph dsg = DatasetGraphFactory.create();
        dsg.add(Quad.create(GRAPH_NAME, BNODE, PRED, OBJ));
        return dsg;
    }

    private static NodeFormatter createCustomPrefixFormatter() {
        return new NodeFormatterNT() {
            @Override
            public void formatBNode(AWriter w, String label) {
                w.print(CUSTOM_BNODE_PREFIX);
                w.print(label);
            }
        };
    }

    static Stream<Arguments> graphFormats() {
        return Stream.of(
            Arguments.of(RDFFormat.NTRIPLES),
            Arguments.of(RDFFormat.NTRIPLES_ASCII),
            Arguments.of(RDFFormat.NTRIPLES_PRETTY),
            Arguments.of(RDFFormat.TURTLE_PRETTY),
            Arguments.of(RDFFormat.TURTLE_BLOCKS),
            Arguments.of(RDFFormat.TURTLE_FLAT),
            Arguments.of(RDFFormat.TURTLE_LONG),
            Arguments.of(RDFFormat.TRIG_LONG)
        );
    }

    static Stream<Arguments> datasetFormats() {
        return Stream.of(
            Arguments.of(RDFFormat.NQUADS),
            Arguments.of(RDFFormat.NQUADS_ASCII),
            Arguments.of(RDFFormat.TRIG_PRETTY),
            Arguments.of(RDFFormat.TRIG_BLOCKS),
            Arguments.of(RDFFormat.TRIG_FLAT)
        );
    }

    @Test
    public void rdfwriter_bnode_default_nt() {
        Graph g = createBNodeGraph();
        String s = RDFWriter.create()
            .source(g)
            .lang(Lang.NT)
            .asString();
        assertTrue(s.contains("_:B" + BNODE_LABEL));
    }

    @Test
    public void rdfwriter_bnode_default_nq() {
        DatasetGraph dsg = createBNodeDataset();
        String s = RDFWriter.create()
            .source(dsg)
            .lang(Lang.NQ)
            .asString();
        assertTrue(s.contains("_:B" + BNODE_LABEL));
    }

    @Test
    public void rdfwriter_bnode_default_ttl_blocks() {
        Graph g = createBNodeGraph();
        String s = RDFWriter.create()
            .source(g)
            .format(RDFFormat.TURTLE_BLOCKS)
            .asString();
        assertTrue(s.contains("_:b0"));
        assertFalse(s.contains("_:" + BNODE_LABEL));
    }

    @ParameterizedTest
    @MethodSource("graphFormats")
    public void rdfwriter_bnode_labels_as_given_graph(RDFFormat format) {
        Graph g = createBNodeGraph();
        String serialized = RDFWriter.create()
            .source(g)
            .format(format)
            .labels(NodeToLabel.createBNodeByLabelAsGiven())
            .asString();
        assertTrue(serialized.contains("_:" + BNODE_LABEL), () -> "Expected _:" + BNODE_LABEL + " in " + format + " output: " + serialized);
        assertFalse(serialized.contains("_:B" + BNODE_LABEL));

        Graph parsed = RDFParser.create()
            .fromString(serialized)
            .lang(format.getLang())
            .labelToNode(LabelToNode.createUseLabelAsGiven())
            .toGraph();
        assertTrue(parsed.contains(BNODE, PRED, OBJ), () -> "Round-trip failed for " + format);
    }

    @ParameterizedTest
    @MethodSource("datasetFormats")
    public void rdfwriter_bnode_labels_as_given_dataset(RDFFormat format) {
        DatasetGraph dsg = createBNodeDataset();
        String serialized = RDFWriter.create()
            .source(dsg)
            .format(format)
            .labels(NodeToLabel.createBNodeByLabelAsGiven())
            .asString();
        assertTrue(serialized.contains("_:" + BNODE_LABEL), () -> "Expected _:" + BNODE_LABEL + " in " + format + " output: " + serialized);
        assertFalse(serialized.contains("_:B" + BNODE_LABEL));

        DatasetGraph parsed = RDFParser.create()
            .fromString(serialized)
            .lang(format.getLang())
            .labelToNode(LabelToNode.createUseLabelAsGiven())
            .toDatasetGraph();
        assertTrue(parsed.contains(GRAPH_NAME, BNODE, PRED, OBJ), () -> "Round-trip failed for " + format);
    }

    @ParameterizedTest
    @MethodSource("graphFormats")
    public void rdfwriter_custom_formatter_graph(RDFFormat format) {
        Graph g = createBNodeGraph();
        String serialized = RDFWriter.create()
            .source(g)
            .format(format)
            .formatter(createCustomPrefixFormatter())
            .asString();
        assertTrue(serialized.contains(CUSTOM_BNODE_PREFIX + BNODE_LABEL), () -> "Expected custom bnode in " + format + " output: " + serialized);
    }

    @ParameterizedTest
    @MethodSource("datasetFormats")
    public void rdfwriter_custom_formatter_dataset(RDFFormat format) {
        DatasetGraph dsg = createBNodeDataset();
        String serialized = RDFWriter.create()
            .source(dsg)
            .format(format)
            .formatter(createCustomPrefixFormatter())
            .asString();
        assertTrue(serialized.contains(CUSTOM_BNODE_PREFIX + BNODE_LABEL), () -> "Expected custom bnode in " + format + " output: " + serialized);
    }

    @Test
    public void rdfwriter_context_symbols() {
        Graph g = createBNodeGraph();
        String s1 = RDFWriter.create()
            .source(g)
            .lang(Lang.NT)
            .set(RIOT.symNodeToLabel, NodeToLabel.createBNodeByLabelAsGiven())
            .asString();
        assertTrue(s1.contains("_:" + BNODE_LABEL));

        String s2 = RDFWriter.create()
            .source(g)
            .lang(Lang.TTL)
            .set(RIOT.symNodeFormatter, createCustomPrefixFormatter())
            .asString();
        assertTrue(s2.contains(CUSTOM_BNODE_PREFIX + BNODE_LABEL));
    }

    @Test
    public void rdfwriter_stream_context_symbols() {
        Graph g = createBNodeGraph();
        Context cxt = RIOT.getContext().copy().set(RIOT.symNodeToLabel, NodeToLabel.createBNodeByLabelAsGiven());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StreamRDFWriter.write(out, g, RDFFormat.NTRIPLES, cxt);
        String s = StrUtils.fromUTF8bytes(out.toByteArray());
        assertTrue(s.contains("_:" + BNODE_LABEL));
    }

    @Test
    public void rdfwriter_builder_clone_and_clear() {
        Graph g = createBNodeGraph();
        RDFWriterBuilder builder = RDFWriter.create()
            .source(g)
            .lang(Lang.NT)
            .labels(NodeToLabel.createBNodeByLabelAsGiven());

        RDFWriterBuilder cloned = builder.clone();
        assertTrue(cloned.asString().contains("_:" + BNODE_LABEL));

        builder.labels(null);
        assertTrue(builder.asString().contains("_:B" + BNODE_LABEL));
    }
}
