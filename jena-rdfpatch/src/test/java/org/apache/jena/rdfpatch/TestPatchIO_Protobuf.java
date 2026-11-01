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

package org.apache.jena.rdfpatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdfpatch.binary.RDFChangesWriterProtobuf;
import org.apache.jena.rdfpatch.binary.RDFPatchReaderProtobuf;
import org.apache.jena.rdfpatch.changes.RDFChangesCollector;
import org.apache.jena.rdfpatch.changes.RDFChangesCounter;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Patch;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Patch_Row;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.graph.GraphFactory;
import org.apache.jena.sparql.sse.SSE;

public class TestPatchIO_Protobuf extends AbstractTestPatchIO {

    private static final String PREFIX_EX = "ex";
    private static final String NS_EX = "http://example.org/";
    private static final Node ID_NODE = NodeFactory.createURI("urn:uuid:test-patch-1");
    private static final Node PREV_NODE = NodeFactory.createURI("urn:uuid:test-patch-0");
    private static final Node GRAPH_NODE = SSE.parseNode(":g1");
    private static final Node SUBJ_NODE = SSE.parseNode("_:s1");
    private static final Node PRED_NODE = SSE.parseNode("<http://example.org/p>");
    private static final Node OBJ_DIR_LANG = SSE.parseNode("'hello'@en--ltr");
    private static final Node OBJ_STAR = SSE.parseNode("<<( _:s1 <http://example.org/p> 42 )>>");

    @Override
    protected void write(OutputStream out, RDFPatch patch) {
        RDFPatchOps.writeProtobuf(out, patch);
    }

    @Override
    protected RDFPatch read(InputStream in) {
        return RDFPatchOps.readProtobuf(in);
    }

    @Test public void protobuf_headersPrefixesAndTxnLifecycle() {
        RDFChangesCollector collector = new RDFChangesCollector();
        collector.start();
        collector.header(RDFPatchConst.ID, ID_NODE);
        collector.header(RDFPatchConst.PREV, PREV_NODE);
        collector.txnBegin();
        collector.addPrefix(null, PREFIX_EX, NS_EX);
        collector.addPrefix(GRAPH_NODE, PREFIX_EX, NS_EX);
        collector.add(null, SUBJ_NODE, PRED_NODE, OBJ_DIR_LANG);
        collector.add(GRAPH_NODE, SUBJ_NODE, PRED_NODE, OBJ_STAR);
        collector.delete(GRAPH_NODE, SUBJ_NODE, PRED_NODE, OBJ_STAR);
        collector.deletePrefix(GRAPH_NODE, PREFIX_EX);
        collector.deletePrefix(null, PREFIX_EX);
        collector.segment();
        collector.txnCommit();
        collector.finish();
        RDFPatch original = collector.getRDFPatch();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RDFPatchOps.writeProtobuf(out, original);
        byte[] bytes = out.toByteArray();

        PatchHeader header = RDFPatchOps.readHeaderProtobuf(new ByteArrayInputStream(bytes));
        assertNotNull(header);
        assertEquals(ID_NODE, header.getId());
        assertEquals(PREV_NODE, header.getPrevious());

        RDFPatch decoded = RDFPatchOps.readProtobuf(new ByteArrayInputStream(bytes));
        assertEquals(original, decoded);
    }

    @Test public void protobuf_txnAbortRoundTrip() {
        List<RDF_Patch_Row> rows = new ArrayList<>();
        AtomicBoolean finished = new AtomicBoolean(false);
        RDFChanges writer = RDFChangesWriterProtobuf.create(rows::add, () -> finished.set(true));
        writer.start();
        writer.txnBegin();
        writer.add(null, SUBJ_NODE, PRED_NODE, OBJ_DIR_LANG);
        writer.txnAbort();
        writer.finish();

        assertTrue(finished.get());
        assertEquals(3, rows.size());

        RDFChangesCounter counter = new RDFChangesCounter();
        Consumer<RDF_Patch_Row> receiver = RDFPatchOps.protobufChangesReceiver(counter);
        counter.start();
        rows.forEach(receiver);
        counter.finish();

        assertEquals(1, counter.summary().countTxnBegin);
        assertEquals(1, counter.summary().countAddData);
        assertEquals(1, counter.summary().countTxnAbort);
    }

    @Test public void protobuf_singleBlockAndApplyChange() {
        Quad q = Quad.create(GRAPH_NODE, SUBJ_NODE, PRED_NODE, OBJ_STAR);
        Triple t = Triple.create(SUBJ_NODE, PRED_NODE, OBJ_DIR_LANG);

        RDFPatch patch = RDFPatchOps.build(c -> {
            c.txnBegin();
            c.add(q.getGraph(), q.getSubject(), q.getPredicate(), q.getObject());
            c.add(null, t.getSubject(), t.getPredicate(), t.getObject());
            c.txnCommit();
        });

        RDF_Patch pbPatch = RDFChangesWriterProtobuf.toProtobuf(patch);
        assertEquals(4, pbPatch.getRowCount());
        RDFPatch fromBlock = RDFPatchOps.readProtobuf(pbPatch);
        assertEquals(patch, fromBlock);

        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
        RDFPatchOps.applyChangeProtobuf(dsg, pbPatch.getRowList().iterator());
        assertTrue(dsg.contains(q));
        assertTrue(dsg.getDefaultGraph().contains(t));

        ByteArrayOutputStream streamOut = new ByteArrayOutputStream();
        RDFPatchOps.writeProtobuf(streamOut, s -> {
            s.start();
            s.triple(t);
            s.finish();
        });

        Graph g = GraphFactory.createTxnGraph();
        RDFPatchOps.applyChangeProtobuf(g, new ByteArrayInputStream(streamOut.toByteArray()));
        assertTrue(g.contains(t));
    }

    @Test public void protobuf_validation() {
        assertThrows(IllegalArgumentException.class, () -> RDFChangesWriterProtobuf.create((Consumer<RDF_Patch_Row>) null));
        assertThrows(IllegalArgumentException.class, () -> RDFChangesWriterProtobuf.create((OutputStream) null));
        assertThrows(IllegalArgumentException.class, () -> RDFChangesWriterProtobuf.toProtobuf(null));
        assertThrows(IllegalArgumentException.class, () -> RDFPatchReaderProtobuf.create((InputStream) null));
        assertThrows(IllegalArgumentException.class, () -> RDFPatchReaderProtobuf.create((RDF_Patch) null));
        assertThrows(IllegalArgumentException.class, () -> RDFPatchReaderProtobuf.changesToRowConsumer(null));
        assertThrows(PatchException.class,
            () -> RDFPatchReaderProtobuf.dispatch(RDF_Patch_Row.getDefaultInstance(), new RDFChangesCounter()));
    }
}
