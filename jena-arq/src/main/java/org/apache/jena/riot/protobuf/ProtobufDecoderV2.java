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

import java.util.Arrays;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.protobuf.wire.PB_RDF.*;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.sparql.core.Quad;

/**
 * Decode terms of a version 2 RDF Protobuf stream.
 * <p>
 * This is the reverse of {@link ProtobufEncoderV2}: the term table and the
 * datatype table are filled from the terms read in full, so a reference is a
 * lookup and a repeat is the term from the previous row. Nodes are created
 * once per table entry, not once per occurrence.
 */
final class ProtobufDecoderV2 {
    // Guard against a corrupt or hostile header allocating huge tables.
    private static final int MAX_TABLE_SIZE = 1<<24;

    private final PrefixMap pmap;
    private Node[] termTable;
    private int nextTerm = 0;
    private RDFDatatype[] datatypeTable;
    private int nextDatatype = 0;
    // Terms of the previous row: subject, predicate, object, graph.
    private final Node[] previous = new Node[4];

    ProtobufDecoderV2(PrefixMap pmap) {
        this.pmap = pmap;
    }

    /** Start, or restart, from a header row. */
    void start(RDF_Header header) {
        start(header.getVersion(), header.getTermTableSize(), header.getDatatypeTableSize());
    }

    /** Start, or restart, from the contents of a header row. */
    void start(int version, int termTableSize, int datatypeTableSize) {
        if ( version != ProtobufEncoderV2.VERSION )
            throw new RiotProtobufException("Unsupported RDF Protobuf stream version: "+version);
        termTable = new Node[tableSize(termTableSize, "term")];
        datatypeTable = new RDFDatatype[tableSize(datatypeTableSize, "datatype")];
        nextTerm = 0;
        nextDatatype = 0;
        Arrays.fill(previous, null);
    }

    /** Whether a header has been seen. */
    boolean started() {
        return termTable != null;
    }

    private static int tableSize(int size, String name) {
        if ( size < 1 || size > MAX_TABLE_SIZE )
            throw new RiotProtobufException("Bad "+name+" table size in stream header: "+size);
        return size;
    }

    Triple triple(RDF_Triple rt) {
        Node s = decode(rt.getS(), 0);
        Node p = decode(rt.getP(), 1);
        Node o = decode(rt.getO(), 2);
        return Triple.create(s, p, o);
    }

    Quad quad(RDF_Quad rq) {
        Node g = rq.hasG() ? decode(rq.getG(), 3) : null;
        Node s = decode(rq.getS(), 0);
        Node p = decode(rq.getP(), 1);
        Node o = decode(rq.getO(), 2);
        return Quad.create(g, s, p, o);
    }

    /** Decode a term in a row position: 0 subject, 1 predicate, 2 object, 3 graph. */
    private Node decode(RDF_Term term, int position) {
        if ( term.getTermCase() == RDF_Term.TermCase.REPEAT )
            return repeat(position);
        Node node = term(term);
        previous(position, node);
        return node;
    }

    // The table operations are also used by ProtobufStreamReader, which parses
    // the wire format directly rather than through RDF_Term objects.

    /** The term of the previous row in this position. */
    Node repeat(int position) {
        Node node = previous[position];
        if ( node == null )
            throw new RiotProtobufException("RDF_REPEAT with no previous term");
        return node;
    }

    /** Record the term of this row in a position. */
    void previous(int position, Node node) {
        previous[position] = node;
    }

    /** The term in a slot of the term table. */
    Node termRef(int slot) {
        if ( slot < 0 || slot >= termTable.length )
            throw new RiotProtobufException("Term reference out of range: "+slot);
        Node node = termTable[slot];
        if ( node == null )
            throw new RiotProtobufException("Term reference to an empty slot: "+slot);
        return node;
    }

    /** The datatype in a slot of the datatype table. */
    RDFDatatype datatypeRef(int slot) {
        if ( slot < 0 || slot >= datatypeTable.length )
            throw new RiotProtobufException("Datatype reference out of range: "+slot);
        RDFDatatype dt = datatypeTable[slot];
        if ( dt == null )
            throw new RiotProtobufException("Datatype reference to an empty slot: "+slot);
        return dt;
    }

    /** Decode a term, using and maintaining the tables. */
    private Node term(RDF_Term term) {
        switch (term.getTermCase()) {
            case TERMREF :
                return termRef(term.getTermRef());
            case IRI :
            case BNODE :
            case PREFIXNAME : {
                Node node = ProtobufConvert.convert(null, term, pmap);
                enterTerm(node);
                return node;
            }
            case LITERAL : {
                RDF_Literal literal = term.getLiteral();
                switch (literal.getLiteralKindCase()) {
                    case DATATYPEREF :
                        return NodeFactory.createLiteralDT(literal.getLex(), datatypeRef(literal.getDatatypeRef()));
                    case DATATYPE : {
                        RDFDatatype dt = NodeFactory.getType(literal.getDatatype());
                        enterDatatype(dt);
                        return NodeFactory.createLiteralDT(literal.getLex(), dt);
                    }
                    default :
                        return ProtobufConvert.convert(null, term, pmap);
                }
            }
            case TRIPLETERM : {
                RDF_Triple rt = term.getTripleTerm();
                Node s = term(rt.getS());
                Node p = term(rt.getP());
                Node o = term(rt.getO());
                return NodeFactory.createTripleTerm(Triple.create(s, p, o));
            }
            case REPEAT :
                throw new RiotProtobufException("RDF_REPEAT inside a triple term");
            default :
                return ProtobufConvert.convert(null, term, pmap);
        }
    }

    /** Add a term read in full to the term table. */
    void enterTerm(Node node) {
        int slot = nextTerm;
        termTable[slot] = node;
        nextTerm = ( slot+1 == termTable.length ) ? 0 : slot+1;
    }

    /** Add a datatype read in full to the datatype table. */
    void enterDatatype(RDFDatatype datatype) {
        int slot = nextDatatype;
        datatypeTable[slot] = datatype;
        nextDatatype = ( slot+1 == datatypeTable.length ) ? 0 : slot+1;
    }
}
