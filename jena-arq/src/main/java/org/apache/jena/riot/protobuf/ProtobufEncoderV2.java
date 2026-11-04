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

import java.util.HashMap;
import java.util.Map;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.protobuf.wire.PB_RDF.*;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapFactory;
import org.apache.jena.sparql.core.Quad;

/**
 * Encode terms for a version 2 RDF Protobuf stream.
 * <p>
 * The encoder keeps the term table, the namespace table, the datatype table and
 * the terms of the previous row, as described in {@code binary-rdf.proto}. A
 * term that is already in the table is written as a reference, a term that is
 * the same as in the previous row is left out, and an IRI written in full is
 * split into its namespace and local name. The reader, {@link ProtobufDecoderV2},
 * maintains the same tables from what it reads, so no table entries need to be
 * sent.
 */
final class ProtobufEncoderV2 {
    static final int VERSION = 2;
    static final int DFT_TERM_TABLE_SIZE = 16*1024;
    static final int DFT_DATATYPE_TABLE_SIZE = 64;
    static final int DFT_NAMESPACE_TABLE_SIZE = 256;

    private static final PrefixMap PMAP0 = PrefixMapFactory.emptyPrefixMap();

    private final boolean encodeValues;

    // Term table, with clock replacement: a slot that has been referenced since
    // its term was placed is passed over once.
    private final Node[] termTable;
    private final boolean[] referenced;
    private final NodeIndex termIndex;
    private int hand = 0;       // The slot to try next.
    private int last = -1;      // The slot of the most recently placed term.

    // Namespace table: slots are filled in order and reused from the start when full.
    private final String[] namespaceTable;
    private final Map<String, Integer> namespaceIndex;
    private int nextNamespace = 0;
    private int lastNamespace = -1;

    // Datatype table, filled in the same way.
    private final String[] datatypeTable;
    private final Map<String, Integer> datatypeIndex;
    private int nextDatatype = 0;

    // Terms of the previous row: subject, predicate, object, graph.
    private final Node[] previous = new Node[4];

    private final RDF_Term.Builder termBuilder = RDF_Term.newBuilder();
    private final RDF_Literal.Builder literalBuilder = RDF_Literal.newBuilder();
    private final RDF_NsIRI.Builder nsIriBuilder = RDF_NsIRI.newBuilder();

    ProtobufEncoderV2(boolean encodeValues) {
        this(encodeValues, DFT_TERM_TABLE_SIZE, DFT_DATATYPE_TABLE_SIZE, DFT_NAMESPACE_TABLE_SIZE);
    }

    ProtobufEncoderV2(boolean encodeValues, int termTableSize, int datatypeTableSize) {
        this(encodeValues, termTableSize, datatypeTableSize, DFT_NAMESPACE_TABLE_SIZE);
    }

    ProtobufEncoderV2(boolean encodeValues, int termTableSize, int datatypeTableSize, int namespaceTableSize) {
        if ( termTableSize < 1 || datatypeTableSize < 1 || namespaceTableSize < 1 )
            throw new IllegalArgumentException("Table sizes must be at least 1");
        this.encodeValues = encodeValues;
        this.termTable = new Node[termTableSize];
        this.referenced = new boolean[termTableSize];
        this.termIndex = new NodeIndex(termTableSize);
        this.namespaceTable = new String[namespaceTableSize];
        this.namespaceIndex = new HashMap<>(2*namespaceTableSize);
        this.datatypeTable = new String[datatypeTableSize];
        this.datatypeIndex = new HashMap<>(2*datatypeTableSize);
    }

    /** The header row that starts the stream. */
    RDF_Header header() {
        return RDF_Header.newBuilder()
                .setVersion(VERSION)
                .setTermTableSize(termTable.length)
                .setDatatypeTableSize(datatypeTable.length)
                .setNamespaceTableSize(namespaceTable.length)
                .build();
    }

    RDF_Triple triple(Triple triple, RDF_Triple.Builder tripleBuilder) {
        tripleBuilder.clear();
        Node s = triple.getSubject();
        if ( !repeat(s, 0) ) {
            int ref = termRef(s);
            if ( ref > 0 ) tripleBuilder.setSRef(ref); else tripleBuilder.setS(full(s));
        }
        Node p = triple.getPredicate();
        if ( !repeat(p, 1) ) {
            int ref = termRef(p);
            if ( ref > 0 ) tripleBuilder.setPRef(ref); else tripleBuilder.setP(full(p));
        }
        Node o = triple.getObject();
        if ( !repeat(o, 2) ) {
            int ref = termRef(o);
            if ( ref > 0 ) tripleBuilder.setORef(ref); else tripleBuilder.setO(full(o));
        }
        return tripleBuilder.build();
    }

    RDF_Quad quad(Quad quad, RDF_Quad.Builder quadBuilder) {
        quadBuilder.clear();
        Node s = quad.getSubject();
        if ( !repeat(s, 0) ) {
            int ref = termRef(s);
            if ( ref > 0 ) quadBuilder.setSRef(ref); else quadBuilder.setS(full(s));
        }
        Node p = quad.getPredicate();
        if ( !repeat(p, 1) ) {
            int ref = termRef(p);
            if ( ref > 0 ) quadBuilder.setPRef(ref); else quadBuilder.setP(full(p));
        }
        Node o = quad.getObject();
        if ( !repeat(o, 2) ) {
            int ref = termRef(o);
            if ( ref > 0 ) quadBuilder.setORef(ref); else quadBuilder.setO(full(o));
        }
        // The graph goes after the subject, predicate and object: the reader meets
        // the fields in that order and the tables must change in the same order.
        // A null graph is written as RDF_UNDEF every time; it is never a repeat.
        Node g = quad.getGraph();
        if ( !repeat(g, 3) ) {
            int ref = termRef(g);
            if ( ref > 0 ) quadBuilder.setGRef(ref); else quadBuilder.setG(full(g));
        }
        return quadBuilder.build();
    }

    // The table operations are also used by ProtobufStreamWriter, which writes
    // the wire format directly rather than through RDF_Term objects.

    /**
     * Whether the term is the same as the term of the previous row in this
     * position. If not, it becomes the previous term for the next row. A null
     * term (the graph of a quad) is never a repeat.
     */
    boolean repeat(Node node, int position) {
        if ( node == null )
            return false;
        Node prev = previous[position];
        // Hash codes are cached by the nodes; comparing them first saves comparing
        // IRIs that share a long prefix, or literals, character by character.
        if ( node == prev || ( prev != null && node.hashCode() == prev.hashCode() && node.equals(prev) ) )
            return true;
        previous[position] = node;
        return false;
    }

    /**
     * The reference for a term in the term table, or -1 if the term is not an IRI
     * or blank node in the table. The slot is marked as referenced, so this must
     * only be called when the reference is then written.
     */
    int termRef(Node node) {
        if ( node == null || !(node.isURI() || node.isBlank()) )
            return -1;
        int slot = termIndex.get(node);
        if ( slot < 0 )
            return -1;
        referenced[slot] = true;
        return distance(last, slot, termTable.length);
    }

    /** The reference for a namespace in the namespace table, or -1. */
    int namespaceRef(String namespace) {
        Integer slot = namespaceIndex.get(namespace);
        return ( slot == null ) ? -1 : distance(lastNamespace, slot, namespaceTable.length);
    }

    /** The slot of a datatype in the datatype table, or -1. */
    int datatypeSlot(String datatype) {
        Integer slot = datatypeIndex.get(datatype);
        return ( slot == null ) ? -1 : slot;
    }

    /** A reference as a distance back from the most recently placed entry: 1 is that entry. */
    private static int distance(int last, int slot, int size) {
        int d = last - slot;
        if ( d < 0 )
            d += size;
        return d+1;
    }

    /**
     * Where an IRI splits into namespace and local name: the index after its last
     * '/' or '#', or 0 if it has neither and is written whole.
     */
    static int namespaceEnd(String iri) {
        for ( int i = iri.length()-1; i >= 0; i-- ) {
            char ch = iri.charAt(i);
            if ( ch == '/' || ch == '#' )
                return i+1;
        }
        return 0;
    }

    /**
     * Encode a term that is written as an {@code RDF_Term}, not as a reference or
     * left out: the caller has checked {@link #termRef} first. The tables are
     * maintained.
     */
    RDF_Term term(Node node) {
        return full(node);
    }

    private RDF_Term full(Node node) {
        if ( node == null ) {
            termBuilder.clear();
            return ProtobufConvert.toProtobuf(node, PMAP0, termBuilder, false);
        }
        if ( node.isURI() || node.isBlank() ) {
            termBuilder.clear();
            RDF_Term term;
            int nsEnd = node.isURI() ? namespaceEnd(node.getURI()) : 0;
            if ( nsEnd > 0 ) {
                String iri = node.getURI();
                String namespace = iri.substring(0, nsEnd);
                nsIriBuilder.clear();
                int nsRef = namespaceRef(namespace);
                if ( nsRef > 0 )
                    nsIriBuilder.setNamespaceRef(nsRef);
                else {
                    nsIriBuilder.setNamespace(namespace);
                    enterNamespace(namespace);
                }
                nsIriBuilder.setLocalName(iri.substring(nsEnd));
                term = termBuilder.setNsIri(nsIriBuilder).build();
            } else
                term = ProtobufConvert.toProtobuf(node, PMAP0, termBuilder, false);
            enterTerm(node);
            return term;
        }
        if ( node.isLiteral() ) {
            termBuilder.clear();
            if ( encodeValues ) {
                RDF_Term value = ProtobufConvert.toProtobufValue(node, termBuilder);
                if ( value != null )
                    return value;
                termBuilder.clear();
            }
            literalBuilder.clear();
            ProtobufConvert.literal(node, PMAP0, literalBuilder);
            if ( literalBuilder.getLiteralKindCase() == RDF_Literal.LiteralKindCase.DATATYPE ) {
                String datatype = literalBuilder.getDatatype();
                Integer slot = datatypeIndex.get(datatype);
                if ( slot != null )
                    literalBuilder.setDatatypeRef(slot);
                else
                    enterDatatype(datatype);
            }
            return termBuilder.setLiteral(literalBuilder).build();
        }
        if ( node.isTripleTerm() ) {
            // The terms inside a triple term use the tables but are never left out.
            Triple triple = node.getTriple();
            RDF_Triple.Builder tripleTerm = RDF_Triple.newBuilder();
            Node s = triple.getSubject();
            int sRef = termRef(s);
            if ( sRef > 0 ) tripleTerm.setSRef(sRef); else tripleTerm.setS(full(s));
            Node p = triple.getPredicate();
            int pRef = termRef(p);
            if ( pRef > 0 ) tripleTerm.setPRef(pRef); else tripleTerm.setP(full(p));
            Node o = triple.getObject();
            int oRef = termRef(o);
            if ( oRef > 0 ) tripleTerm.setORef(oRef); else tripleTerm.setO(full(o));
            return RDF_Term.newBuilder().setTripleTerm(tripleTerm).build();
        }
        // Variables and ANY.
        termBuilder.clear();
        return ProtobufConvert.toProtobuf(node, PMAP0, termBuilder, false);
    }

    /** Add a term written in full to the term table. */
    void enterTerm(Node node) {
        int slot = hand;
        // Pass over slots referenced since their term was placed, clearing the mark.
        while ( referenced[slot] ) {
            referenced[slot] = false;
            slot = ( slot+1 == termTable.length ) ? 0 : slot+1;
        }
        Node evicted = termTable[slot];
        if ( evicted != null )
            termIndex.remove(evicted);
        termTable[slot] = node;
        termIndex.put(node, slot);
        last = slot;
        hand = ( slot+1 == termTable.length ) ? 0 : slot+1;
    }

    /** Add a namespace written in full to the namespace table. */
    void enterNamespace(String namespace) {
        int slot = nextNamespace;
        String evicted = namespaceTable[slot];
        if ( evicted != null )
            namespaceIndex.remove(evicted);
        namespaceTable[slot] = namespace;
        namespaceIndex.put(namespace, slot);
        lastNamespace = slot;
        nextNamespace = ( slot+1 == namespaceTable.length ) ? 0 : slot+1;
    }

    /** Add a datatype written in full to the datatype table. */
    void enterDatatype(String datatype) {
        int slot = nextDatatype;
        String evicted = datatypeTable[slot];
        if ( evicted != null )
            datatypeIndex.remove(evicted);
        datatypeTable[slot] = datatype;
        datatypeIndex.put(datatype, slot);
        nextDatatype = ( slot+1 == datatypeTable.length ) ? 0 : slot+1;
    }

    /**
     * Map from a term to its slot in the term table.
     * <p>
     * An open-addressing hash table with linear probing, sized for the term table,
     * so that the lookup, insert and remove done for most terms of most rows do
     * not allocate (a {@code HashMap} would box the slot and create an entry for
     * each insert) and removal does not leave tombstones.
     */
    private static final class NodeIndex {
        private final Node[] keys;
        // Hash codes of the keys: compared before equals(), because IRIs in one
        // stream often share a long prefix and comparing them is not cheap.
        private final int[] hashes;
        private final int[] values;
        private final int mask;
        private final int shift;

        NodeIndex(int capacity) {
            // At most half full.
            int size = Integer.highestOneBit(Math.max(capacity, 8) * 2 - 1) << 1;
            keys = new Node[size];
            hashes = new int[size];
            values = new int[size];
            mask = size - 1;
            shift = Integer.numberOfLeadingZeros(size) + 1;
        }

        // Fibonacci hashing: IRIs that differ only in their last characters have
        // close hash codes, which linear probing would turn into long runs.
        private int indexFor(int hash) {
            return (hash * 0x9E3779B9) >>> shift;
        }

        private boolean matches(int i, Node key, int hash) {
            Node k = keys[i];
            return k == key || ( hashes[i] == hash && k.equals(key) );
        }

        /** The slot for the term, or -1. */
        int get(Node key) {
            int hash = key.hashCode();
            for ( int i = indexFor(hash); ; i = (i+1) & mask ) {
                if ( keys[i] == null )
                    return -1;
                if ( matches(i, key, hash) )
                    return values[i];
            }
        }

        void put(Node key, int value) {
            int hash = key.hashCode();
            for ( int i = indexFor(hash); ; i = (i+1) & mask ) {
                if ( keys[i] == null ) {
                    keys[i] = key;
                    hashes[i] = hash;
                    values[i] = value;
                    return;
                }
                if ( matches(i, key, hash) ) {
                    values[i] = value;
                    return;
                }
            }
        }

        void remove(Node key) {
            int hash = key.hashCode();
            int i = indexFor(hash);
            for ( ; ; i = (i+1) & mask ) {
                if ( keys[i] == null )
                    return;
                if ( matches(i, key, hash) )
                    break;
            }
            // Close the gap: move back any later entries of the same probe run.
            int j = i;
            while (true) {
                j = (j+1) & mask;
                if ( keys[j] == null )
                    break;
                int home = indexFor(hashes[j]);
                // The entry at j can move to i if its home is not in (i, j].
                boolean move = ( i <= j ) ? ( home <= i || home > j ) : ( home <= i && home > j );
                if ( move ) {
                    keys[i] = keys[j];
                    hashes[i] = hashes[j];
                    values[i] = values[j];
                    i = j;
                }
            }
            keys[i] = null;
        }
    }
}
