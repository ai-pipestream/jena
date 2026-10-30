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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.lib.Cache;
import org.apache.jena.atlas.lib.CacheFactory;
import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Stream;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_StreamRow;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Term.TermCase;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapFactory;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFBase;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.riot.system.StreamRDFOps;
import org.apache.jena.riot.system.StreamRDFWriter;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.graph.GraphFactory;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.sparql.util.IsoMatcher;

public class TestProtobufStreamRDF {

    static String gs = StrUtils.strjoinNL(
        "(graph",
        "  (_:a :p 123) ",
        "  (_:a :p 'foo'@en) ",
        "  (_:b :p '456') ",        // Preserved values only.
        "  (_:b :p '456.5') ",
        "  (_:b :p '456.5e6') ",
         ")");

    static Graph graph = SSE.parseGraph(gs);

    static String dgs = StrUtils.strjoinNL(
        "(dataset",
        "  (graph (:s1 :p _:a) (:s2 :p _:a))" ,
        "  (graph :g  (:s1 :p _:a))" ,
        "  (graph _:a (:s2 :p _:a))" ,
        ")" );

    static DatasetGraph datasetGraph = SSE.parseDatasetGraph(dgs);

    // Using delimited streams.
    @Test public void graph_01() throws IOException {
        testGraph(true);
    }

    // Using a single marshaled block
    @Test public void graph_02() throws IOException {
        testGraph(false);
    }

    private void testGraph(boolean delimited) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        if ( delimited ) {
            StreamRDF stream = ProtobufRDF.streamToOutputStream(out);// , true); // With values.
            StreamRDFOps.graphToStream(graph, stream);
        } else {
            ProtobufRDF_Blk.streamToOutputStreamBlk(out, s -> StreamRDFOps.graphToStream(graph, s));// , true); // With values.
        }

        byte[] bytes = out.toByteArray();
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);


      Graph g2 = GraphFactory.createGraphMem();


      StreamRDF stream2 = StreamRDFLib.graph(g2);

      // Delimited
      if ( delimited )
          ProtobufRDF.inputStreamToStreamRDF(in, stream2);
      else
          ProtobufRDF_Blk.inputStreamBlkToStreamRDF(in, stream2);

      //assertTrue(graph.isIsomorphicWith(g2));
      boolean b = IsoMatcher.isomorphic(graph, g2);
      if ( !b ) {
          RDFDataMgr.write(System.out, graph, Lang.TTL);
          System.out.println("---------");
          RDFDataMgr.write(System.out, g2, Lang.TTL);
          System.out.println("=========");
      }

      assertTrue(b);

      // Stronger - same bNodes.
      sameTerms(graph, g2);
    }

    // Using delimited streams.


//    // graph_01 and graph_02 are the same test but use different ways to read/write the graph.
//    // Ditto dataset_01 and dataset_02
//
//    @Test public void graph_01() {
//        ByteArrayOutputStream out = new ByteArrayOutputStream();
//        StreamRDF stream = BinRDF.streamToOutputStream(out, true); // With values.
//        StreamRDFOps.graphToStream(graph, stream);
//
//        byte[] bytes = out.toByteArray();
//        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
//
//        Graph g2 = GraphFactory.createGraphMem();
//        StreamRDF stream2 = StreamRDFLib.graph(g2);
//        BinRDF.inputStreamToStream(in, stream2);
//
//        //assertTrue(graph.isIsomorphicWith(g2));
//        boolean b = IsoMatcher.isomorphic(graph, g2);
//        if ( !b ) {
//            RDFDataMgr.write(System.out, graph, Lang.TTL);
//            System.out.println("---------");
//            RDFDataMgr.write(System.out, g2, Lang.TTL);
//            System.out.println("=========");
//        }
//
//        assertTrue(b);
//
//        // Stronger - same bNodes.
//        sameTerms(graph, g2);
//    }
//
//    @Test public void graph_02() {
//        ByteArrayOutputStream out = new ByteArrayOutputStream();
//        StreamRDFWriter.write(out, graph, Lang.RDFTHRIFT, null);
//
//        byte[] bytes = out.toByteArray();
//        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
//
//
//        Graph g2 = GraphFactory.createGraphMem();
//        RDFDataMgr.read(g2, in, Lang.RDFTHRIFT);
//        boolean b = IsoMatcher.isomorphic(graph, g2);
//        assertTrue(b);
//
//        // Stronger - same bNodes.
//        // ** Java8
//        //graph.find(null, null, null).forEachRemaining(t -> assertTrue(g2.contains(t)));
//
//        // Stronger - same bNodes.
//        sameTerms(graph, g2);
//    }
//
//    @Test public void dataset_01() {
//        DatasetGraph dsg1 = datasetGraph;
//        ByteArrayOutputStream out = new ByteArrayOutputStream();
//        StreamRDF stream = BinRDF.streamToOutputStream(out);
//        StreamRDFOps.datasetToStream(dsg1, stream);
//
//        byte[] bytes = out.toByteArray();
//        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
//        DatasetGraph dsg2 = DatasetGraphFactory.create();
//        StreamRDF stream2 = StreamRDFLib.dataset(dsg2);
//        BinRDF.inputStreamToStream(in, stream2);
//
//        boolean b = IsoMatcher.isomorphic(dsg1, dsg2);
//        assertTrue(b);
//        // Stronger - same bNode and same as in original data.
//        Node obj = Iter.first(dsg1.listGraphNodes(), Node::isBlank);
//        termAsObject(dsg1, obj);
//    }
//
//    @Test public void dataset_02() {
//        DatasetGraph dsg1 = datasetGraph;
//        ByteArrayOutputStream out = new ByteArrayOutputStream();
//        StreamRDFWriter.write(out, dsg1, Lang.RDFTHRIFT);
//
//        byte[] bytes = out.toByteArray();
//        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
//        DatasetGraph dsg2 = DatasetGraphFactory.create();
//
//        StreamRDF stream2 = StreamRDFLib.dataset(dsg2);
//        BinRDF.inputStreamToStream(in, stream2);
//
//        boolean b = IsoMatcher.isomorphic(dsg1, dsg2);
//        assertTrue(b);
//        // Stronger - same bNode and same as in original data.
//        Node obj = Iter.first(dsg1.listGraphNodes(), Node::isBlank);
//        termAsObject(dsg1, obj);
//    }
//

    static void sameTerms(Graph g1, Graph g2) {
        assertEquals(g1.size() , g2.size() );
        // ** Java8
        //g1.find(null, null, null).forEachRemaining(t -> assertTrue(g2.contains(t)));
        Iterator<Triple> iter = g1.find(null, null, null);
        while(iter.hasNext()) {
            Triple t = iter.next();
            assertTrue(g2.contains(t));
        }
    }

    static void termAsObject(DatasetGraph dsg, Node term)  {
        Iterator<Quad> iter = dsg.find();
        for (; iter.hasNext(); ) {
            Quad quad = iter.next();
            if ( quad.getObject().equals(term) )
                return;
        }
        fail("Failed to find "+term);
    }

    @Test public void dataset_01() {
        DatasetGraph dsg1 = datasetGraph;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StreamRDF stream = ProtobufRDF.streamToOutputStream(out);
        StreamRDFOps.datasetToStream(dsg1, stream);

        byte[] bytes = out.toByteArray();
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DatasetGraph dsg2 = DatasetGraphFactory.create();
        StreamRDF stream2 = StreamRDFLib.dataset(dsg2);
        ProtobufRDF.inputStreamToStreamRDF(in, stream2);

        boolean b = IsoMatcher.isomorphic(dsg1, dsg2);
        assertTrue(b);
        Node obj = Iter.first(dsg1.listGraphNodes(), Node::isBlank);
        termAsObject(dsg2, obj);
    }

    @Test public void dataset_02() {
        DatasetGraph dsg1 = datasetGraph;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StreamRDFWriter.write(out, dsg1, Lang.RDFPROTO);

        byte[] bytes = out.toByteArray();
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DatasetGraph dsg2 = DatasetGraphFactory.create();
        StreamRDF stream2 = StreamRDFLib.dataset(dsg2);
        ProtobufRDF.inputStreamToStreamRDF(in, stream2);

        boolean b = IsoMatcher.isomorphic(dsg1, dsg2);
        assertTrue(b);
        Node obj = Iter.first(dsg1.listGraphNodes(), Node::isBlank);
        termAsObject(dsg2, obj);
    }

    private static final String EX_PREFIX = "ex";
    private static final String EX_NS = "http://example.org/ns#";
    private static final String EX_PRED = EX_NS + "pred";
    private static final String BASE_IRI = "http://example.org/base/";

    @Test public void streamRowAdapter_graphAndDataset() {
        List<RDF_StreamRow> rows = new ArrayList<>();
        AtomicBoolean finished = new AtomicBoolean(false);
        StreamRDF encoder = ProtobufRDF.streamToRowConsumer(rows::add, false, () -> finished.set(true));

        Triple starTriple = SSE.parseTriple("(<<( _:a :p 123 )>> :q 'hello'@en--ltr)");
        Quad namedQuad = SSE.parseQuad("(:g1 _:a <" + EX_PRED + "> 42)");

        encoder.start();
        encoder.base(BASE_IRI);
        encoder.prefix(EX_PREFIX, EX_NS);
        StreamRDFOps.sendGraphToStream(graph, encoder);
        encoder.triple(starTriple);
        encoder.quad(namedQuad);
        encoder.finish();

        assertTrue(finished.get());
        assertTrue(rows.stream().anyMatch(RDF_StreamRow::hasBase));
        assertTrue(rows.stream().anyMatch(RDF_StreamRow::hasPrefixDecl));
        // Prefix compression must abbreviate http://example.org/ns#pred into a prefixName term.
        assertTrue(rows.stream()
            .filter(RDF_StreamRow::hasQuad)
            .anyMatch(r -> r.getQuad().getP().hasPrefixName()));

        DatasetGraph decodedDsg = DatasetGraphFactory.create();
        List<String> bases = new ArrayList<>();
        StreamRDF sink = new StreamRDFBase() {
            private final StreamRDF inner = StreamRDFLib.dataset(decodedDsg);
            @Override public void base(String base) { bases.add(base); }
            @Override public void prefix(String prefix, String iri) { inner.prefix(prefix, iri); }
            @Override public void triple(Triple triple) { inner.triple(triple); }
            @Override public void quad(Quad quad) { inner.quad(quad); }
        };

        Cache<String, Node> customCache = CacheFactory.createSimpleCache(64);
        PrefixMap pmap = PrefixMapFactory.create();
        Consumer<RDF_StreamRow> decoder = ProtobufRDF.rowConsumerToStreamRDF(pmap, sink, customCache);
        sink.start();
        rows.forEach(decoder);
        sink.finish();

        assertEquals(List.of(BASE_IRI), bases);
        assertEquals(EX_NS, decodedDsg.prefixes().get(EX_PREFIX));
        assertTrue(decodedDsg.getDefaultGraph().contains(starTriple));
        assertTrue(decodedDsg.contains(namedQuad));
        Node cachedPred = customCache.getIfPresent(EX_PRED);
        assertNotNull(cachedPred);
        assertSame(cachedPred, decodedDsg.find(namedQuad.getGraph(), null, null, null).next().getPredicate());
    }

    @Test public void streamRowAdapter_withValues() {
        List<RDF_StreamRow> rowsWithValues = new ArrayList<>();
        StreamRDF encoderVal = ProtobufRDF.streamToRowConsumer(rowsWithValues::add, true);
        Triple intTriple = SSE.parseTriple("(:s :p 123)");
        encoderVal.start();
        encoderVal.triple(intTriple);
        encoderVal.finish();

        assertEquals(1, rowsWithValues.size());
        assertEquals(TermCase.VALINTEGER, rowsWithValues.get(0).getTriple().getO().getTermCase());
        assertEquals(123L, rowsWithValues.get(0).getTriple().getO().getValInteger());

        List<RDF_StreamRow> rowsLex = new ArrayList<>();
        StreamRDF encoderLex = ProtobufRDF.streamToRowConsumer(rowsLex::add, false);
        encoderLex.start();
        encoderLex.triple(intTriple);
        encoderLex.finish();

        assertEquals(1, rowsLex.size());
        assertEquals(TermCase.LITERAL, rowsLex.get(0).getTriple().getO().getTermCase());
    }

    @Test public void streamBatchAdapter_batchSizeAndFlush() {
        List<RDF_Stream> batches = new ArrayList<>();
        StreamRDF encoder = ProtobufRDF.streamToBatchConsumer(batches::add, 2, true);
        StreamRDFOps.datasetToStream(datasetGraph, encoder);

        // datasetGraph has 4 quads/triples total; with batchSize=2 that yields 2 full batches of 2.
        assertEquals(2, batches.size());
        assertEquals(2, batches.get(0).getRowCount());
        assertEquals(2, batches.get(1).getRowCount());

        // Test partial final batch with batchSize=3.
        List<RDF_Stream> batchesPartial = new ArrayList<>();
        StreamRDF encoderPartial = ProtobufRDF.streamToBatchConsumer(batchesPartial::add, 3, false);
        StreamRDFOps.datasetToStream(datasetGraph, encoderPartial);
        assertEquals(2, batchesPartial.size());
        assertEquals(3, batchesPartial.get(0).getRowCount());
        assertEquals(1, batchesPartial.get(1).getRowCount());

        DatasetGraph decoded = DatasetGraphFactory.create();
        StreamRDF dest = StreamRDFLib.dataset(decoded);
        Consumer<RDF_Stream> batchDecoder = ProtobufRDF.batchConsumerToStreamRDF(dest);
        dest.start();
        batchesPartial.forEach(batchDecoder);
        dest.finish();

        assertTrue(IsoMatcher.isomorphic(datasetGraph, decoded));
        Node obj = Iter.first(datasetGraph.listGraphNodes(), Node::isBlank);
        termAsObject(decoded, obj);
    }

    @Test public void protobufToStreamRDF_singleMessage() {
        List<RDF_Stream> batches = new ArrayList<>();
        StreamRDF encoder = ProtobufRDF.streamToBatchConsumer(batches::add, 100, true);
        StreamRDFOps.graphToStream(graph, encoder);
        assertEquals(1, batches.size());

        Graph decoded = GraphFactory.createGraphMem();
        ProtobufRDF.protobufToStreamRDF(batches.get(0), StreamRDFLib.graph(decoded));
        sameTerms(graph, decoded);
    }

    @Test public void streamAdapters_validation() {
        assertThrows(IllegalArgumentException.class, () -> ProtobufRDF.streamToRowConsumer(null));
        assertThrows(IllegalArgumentException.class, () -> ProtobufRDF.streamToBatchConsumer(null));
        assertThrows(IllegalArgumentException.class, () -> ProtobufRDF.streamToBatchConsumer(b -> {}, 0));
        assertThrows(IllegalArgumentException.class, () -> ProtobufRDF.streamToBatchConsumer(b -> {}, -5));
        assertThrows(IllegalArgumentException.class, () -> ProtobufRDF.rowConsumerToStreamRDF(null));
        assertThrows(IllegalArgumentException.class, () -> ProtobufRDF.batchConsumerToStreamRDF(null));
        assertThrows(IllegalArgumentException.class, () -> ProtobufRDF.protobufToStreamRDF(null, StreamRDFLib.sinkNull()));
        assertThrows(IllegalArgumentException.class, () -> ProtobufRDF.protobufToStreamRDF(RDF_Stream.getDefaultInstance(), null));
    }
//
//    // ** Java8
////    public static <T> Stream<T> stream(Iterator<? extends T> iterator) {
////        int characteristics = Spliterator.ORDERED | Spliterator.IMMUTABLE;
////        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, characteristics), false);
////    }
}

