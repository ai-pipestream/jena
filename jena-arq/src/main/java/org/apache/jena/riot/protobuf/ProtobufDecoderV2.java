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
import org.apache.jena.riot.system.RiotLib;
import org.apache.jena.sparql.core.Quad;

/**
 * Decode terms of a version 2 RDF Protobuf stream.
 * <p>
 * This is the reverse of {@link ProtobufEncoderV2}: the term, namespace and
 * datatype tables are filled from what is read in full, so a reference is a
 * lookup and a term left out is the term from the previous row. Nodes are
 * created once per table entry, not once per occurrence.
 */
final class ProtobufDecoderV2 {
    // Guard against a corrupt or hostile header allocating huge tables.
    private static final int MAX_TABLE_SIZE = 1<<24;

    private final PrefixMap pmap;
    // Term table, with clock replacement as in the encoder.
    private Node[] termTable;
    private boolean[] referenced;
    private int hand = 0;
    private int last = -1;
    private String[] namespaceTable;
    private int nextNamespace = 0;
    private int lastNamespace = -1;
    private RDFDatatype[] datatypeTable;
    private int nextDatatype = 0;
    // Terms of the previous row: subject, predicate, object, graph.
    private final Node[] previous = new Node[4];

    ProtobufDecoderV2(PrefixMap pmap) {
        this.pmap = pmap;
    }

    /** Start, or restart, from a header row. */
    void start(RDF_Header header) {
        start(header.getVersion(), header.getTermTableSize(), header.getDatatypeTableSize(), header.getNamespaceTableSize());
    }

    /** Start, or restart, from the contents of a header row. */
    void start(int version, int termTableSize, int datatypeTableSize, int namespaceTableSize) {
        if ( version != ProtobufEncoderV2.VERSION )
            throw new RiotProtobufException("Unsupported RDF Protobuf stream version: "+version);
        termTable = new Node[tableSize(termTableSize, "term")];
        referenced = new boolean[termTable.length];
        namespaceTable = new String[tableSize(namespaceTableSize, "namespace")];
        datatypeTable = new RDFDatatype[tableSize(datatypeTableSize, "datatype")];
        hand = 0;
        last = -1;
        nextNamespace = 0;
        lastNamespace = -1;
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
        Node s = rt.hasS() ? decode(rt.getS(), 0) : ( rt.getSRef() != 0 ) ? reference(rt.getSRef(), 0) : repeat(0);
        Node p = rt.hasP() ? decode(rt.getP(), 1) : ( rt.getPRef() != 0 ) ? reference(rt.getPRef(), 1) : repeat(1);
        Node o = rt.hasO() ? decode(rt.getO(), 2) : ( rt.getORef() != 0 ) ? reference(rt.getORef(), 2) : repeat(2);
        return Triple.create(s, p, o);
    }

    Quad quad(RDF_Quad rq) {
        Node s = rq.hasS() ? decode(rq.getS(), 0) : ( rq.getSRef() != 0 ) ? reference(rq.getSRef(), 0) : repeat(0);
        Node p = rq.hasP() ? decode(rq.getP(), 1) : ( rq.getPRef() != 0 ) ? reference(rq.getPRef(), 1) : repeat(1);
        Node o = rq.hasO() ? decode(rq.getO(), 2) : ( rq.getORef() != 0 ) ? reference(rq.getORef(), 2) : repeat(2);
        Node g = rq.hasG() ? decode(rq.getG(), 3) : ( rq.getGRef() != 0 ) ? reference(rq.getGRef(), 3) : repeat(3);
        return Quad.create(g, s, p, o);
    }

    /** Decode a term written in full in a row position: 0 subject, 1 predicate, 2 object, 3 graph. */
    private Node decode(RDF_Term term, int position) {
        Node node = term(term);
        previous(position, node);
        return node;
    }

    /** Decode a term reference in a row position. */
    private Node reference(int distance, int position) {
        Node node = termRef(distance);
        previous(position, node);
        return node;
    }

    // The table operations are also used by ProtobufStreamReader, which parses
    // the wire format directly rather than through RDF_Term objects.

    /** The term of the previous row in this position. */
    Node repeat(int position) {
        Node node = previous[position];
        if ( node == null )
            throw new RiotProtobufException("Term left out with no previous term");
        return node;
    }

    /** Record the term of this row in a position. A null term (the graph of a quad) is not recorded. */
    void previous(int position, Node node) {
        if ( node != null )
            previous[position] = node;
    }

    /** The term for a reference: a distance back from the most recently placed term. */
    Node termRef(int distance) {
        if ( distance < 1 || distance > termTable.length )
            throw new RiotProtobufException("Term reference out of range: "+distance);
        int slot = last - (distance-1);
        if ( slot < 0 )
            slot += termTable.length;
        Node node = termTable[slot];
        if ( node == null )
            throw new RiotProtobufException("Term reference to an empty slot: "+distance);
        referenced[slot] = true;
        return node;
    }

    /** The namespace for a reference: a distance back from the most recently placed namespace. */
    String namespaceRef(int distance) {
        if ( distance < 1 || distance > namespaceTable.length )
            throw new RiotProtobufException("Namespace reference out of range: "+distance);
        int slot = lastNamespace - (distance-1);
        if ( slot < 0 )
            slot += namespaceTable.length;
        String namespace = namespaceTable[slot];
        if ( namespace == null )
            throw new RiotProtobufException("Namespace reference to an empty slot: "+distance);
        return namespace;
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
            case NSIRI : {
                RDF_NsIRI nsIri = term.getNsIri();
                String namespace;
                if ( nsIri.getNsCase() == RDF_NsIRI.NsCase.NAMESPACEREF )
                    namespace = namespaceRef(nsIri.getNamespaceRef());
                else {
                    namespace = nsIri.getNamespace();
                    enterNamespace(namespace);
                }
                Node node = iri(namespace, nsIri.getLocalName());
                enterTerm(node);
                return node;
            }
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
                Node s = tripleTermPart(rt.hasS(), rt.getS(), rt.getSRef());
                Node p = tripleTermPart(rt.hasP(), rt.getP(), rt.getPRef());
                Node o = tripleTermPart(rt.hasO(), rt.getO(), rt.getORef());
                return NodeFactory.createTripleTerm(Triple.create(s, p, o));
            }
            case REPEAT :
                throw new RiotProtobufException("RDF_REPEAT is not used in a version 2 stream");
            default :
                return ProtobufConvert.convert(null, term, pmap);
        }
    }

    /** A term of a triple term: in full or a reference, never left out. */
    private Node tripleTermPart(boolean present, RDF_Term term, int ref) {
        if ( present )
            return term(term);
        if ( ref != 0 )
            return termRef(ref);
        throw new RiotProtobufException("Triple term with a missing term");
    }

    /** The node for a namespace and local name read in full. */
    Node iri(String namespace, String localName) {
        return RiotLib.createIRIorBNode(namespace.concat(localName));
    }

    /** Add a term read in full to the term table. */
    void enterTerm(Node node) {
        int slot = hand;
        while ( referenced[slot] ) {
            referenced[slot] = false;
            slot = ( slot+1 == termTable.length ) ? 0 : slot+1;
        }
        termTable[slot] = node;
        last = slot;
        hand = ( slot+1 == termTable.length ) ? 0 : slot+1;
    }

    /** Add a namespace read in full to the namespace table. */
    void enterNamespace(String namespace) {
        int slot = nextNamespace;
        namespaceTable[slot] = namespace;
        lastNamespace = slot;
        nextNamespace = ( slot+1 == namespaceTable.length ) ? 0 : slot+1;
    }

    /** Add a datatype read in full to the datatype table. */
    void enterDatatype(RDFDatatype datatype) {
        int slot = nextDatatype;
        datatypeTable[slot] = datatype;
        nextDatatype = ( slot+1 == datatypeTable.length ) ? 0 : slot+1;
    }
}
