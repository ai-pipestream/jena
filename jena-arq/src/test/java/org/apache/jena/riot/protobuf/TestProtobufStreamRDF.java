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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.junit.jupiter.api.Test;

import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_StreamRow;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Term;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Triple;
import org.apache.jena.riot.system.PrefixMapFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.riot.system.StreamRDFOps;
import org.apache.jena.sparql.core.DatasetGraph;
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

    // Wire compatibility: the delimited stream must be the same bytes as
    // the protobuf library's own writeDelimitedTo/parseDelimitedFrom produce
    // and accept, because files written by earlier versions of Jena used them.

    @Test public void delimited_wire_read_compat() throws IOException {
        // Written with the protobuf library, one message at a time.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for ( RDF_StreamRow row : rows() )
            row.writeDelimitedTo(out);
        // Read with Jena.
        Graph g2 = GraphFactory.createGraphMem();
        ProtobufRDF.inputStreamToStreamRDF(new ByteArrayInputStream(out.toByteArray()), StreamRDFLib.graph(g2));
        assertTrue(IsoMatcher.isomorphic(graph, g2));
        sameTerms(graph, g2);
    }

    @Test public void delimited_wire_write_compat() throws IOException {
        // Written with Jena.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StreamRDF stream = ProtobufRDF.streamToOutputStream(out);
        StreamRDFOps.graphToStream(graph, stream);
        // Read with the protobuf library, one message at a time.
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        Graph g2 = GraphFactory.createGraphMem();
        Protobuf2StreamRDF visitor = new Protobuf2StreamRDF(PrefixMapFactory.create(), StreamRDFLib.graph(g2));
        int count = 0;
        while ( true ) {
            RDF_StreamRow row = RDF_StreamRow.parseDelimitedFrom(in);
            if ( row == null )
                break;
            PBufRDF.visit(row, visitor);
            count++;
        }
        assertEquals(graph.size(), count);
        assertTrue(IsoMatcher.isomorphic(graph, g2));
        sameTerms(graph, g2);
    }

    @Test public void delimited_empty_stream() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StreamRDF stream = ProtobufRDF.streamToOutputStream(out);
        stream.start();
        stream.finish();
        assertEquals(0, out.size());
        Graph g2 = GraphFactory.createGraphMem();
        ProtobufRDF.inputStreamToStreamRDF(new ByteArrayInputStream(out.toByteArray()), StreamRDFLib.graph(g2));
        assertEquals(0, g2.size());
    }

    // Many rows: more than one refill of the shared input buffer.
    @Test public void delimited_many_rows() {
        Graph big = GraphFactory.createGraphMem();
        Node p = SSE.parseNode(":p");
        for ( int i = 0 ; i < 20_000 ; i++ ) {
            Node s = NodeFactory.createURI("http://example/subject/with/a/long/enough/name/" + i);
            Node o = NodeFactory.createLiteralString("object value " + i);
            big.add(Triple.create(s, p, o));
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StreamRDF stream = ProtobufRDF.streamToOutputStream(out);
        StreamRDFOps.graphToStream(big, stream);
        Graph g2 = GraphFactory.createGraphMem();
        ProtobufRDF.inputStreamToStreamRDF(new ByteArrayInputStream(out.toByteArray()), StreamRDFLib.graph(g2));
        assertEquals(big.size(), g2.size());
        sameTerms(big, g2);
    }

    private static java.util.List<RDF_StreamRow> rows() {
        java.util.List<RDF_StreamRow> rows = new java.util.ArrayList<>();
        RDF_StreamRow.Builder rowBuilder = RDF_StreamRow.newBuilder();
        RDF_Triple.Builder tripleBuilder = RDF_Triple.newBuilder();
        RDF_Term.Builder termBuilder = RDF_Term.newBuilder();
        graph.find().forEachRemaining(t -> {
            rowBuilder.clear();
            rowBuilder.setTriple(PBufRDF.rdfTriple(t, tripleBuilder, termBuilder));
            rows.add(rowBuilder.build());
        });
        return rows;
    }

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
            assertTrue(g2.contains(t), "Missing: "+t);
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
//
//    // ** Java8
////    public static <T> Stream<T> stream(Iterator<? extends T> iterator) {
////        int characteristics = Spliterator.ORDERED | Spliterator.IMMUTABLE;
////        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, characteristics), false);
////    }
}

