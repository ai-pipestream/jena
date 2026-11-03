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
 * The encoder keeps the term table, the datatype table and the terms of the
 * previous row, as described in {@code binary-rdf.proto}. A term that is already
 * in the table is written as a reference to its slot, and a term that is the
 * same as in the previous row is written as {@code RDF_REPEAT}. The reader,
 * {@link ProtobufDecoderV2}, maintains the same tables from what it reads, so
 * no table entries need to be sent.
 */
final class ProtobufEncoderV2 {
    static final int VERSION = 2;
    static final int DFT_TERM_TABLE_SIZE = 16*1024;
    static final int DFT_DATATYPE_TABLE_SIZE = 64;

    private static final RDF_Term REPEAT = RDF_Term.newBuilder().setRepeat(RDF_REPEAT.getDefaultInstance()).build();
    private static final PrefixMap PMAP0 = PrefixMapFactory.emptyPrefixMap();

    private final boolean encodeValues;

    // Term table: slots are filled in order and reused from the start when full.
    private final Node[] termTable;
    private final NodeIndex termIndex;
    private int nextTerm = 0;
    // A reference only carries a slot number and wire objects are immutable,
    // so one object per slot serves for the whole stream.
    private final RDF_Term[] references;

    // Datatype table, filled in the same way.
    private final String[] datatypeTable;
    private final Map<String, Integer> datatypeIndex;
    private int nextDatatype = 0;

    // Terms of the previous row: subject, predicate, object, graph.
    private final Node[] previous = new Node[4];

    private final RDF_Term.Builder termBuilder = RDF_Term.newBuilder();
    private final RDF_Literal.Builder literalBuilder = RDF_Literal.newBuilder();

    ProtobufEncoderV2(boolean encodeValues) {
        this(encodeValues, DFT_TERM_TABLE_SIZE, DFT_DATATYPE_TABLE_SIZE);
    }

    ProtobufEncoderV2(boolean encodeValues, int termTableSize, int datatypeTableSize) {
        if ( termTableSize < 1 || datatypeTableSize < 1 )
            throw new IllegalArgumentException("Table sizes must be at least 1");
        this.encodeValues = encodeValues;
        this.termTable = new Node[termTableSize];
        this.termIndex = new NodeIndex(termTableSize);
        this.references = new RDF_Term[termTableSize];
        this.datatypeTable = new String[datatypeTableSize];
        this.datatypeIndex = new HashMap<>(2*datatypeTableSize);
    }

    /** The header row that starts the stream. */
    RDF_Header header() {
        return RDF_Header.newBuilder()
                .setVersion(VERSION)
                .setTermTableSize(termTable.length)
                .setDatatypeTableSize(datatypeTable.length)
                .build();
    }

    RDF_Triple triple(Triple triple, RDF_Triple.Builder tripleBuilder) {
        tripleBuilder.clear();
        tripleBuilder.setS(encode(triple.getSubject(), 0));
        tripleBuilder.setP(encode(triple.getPredicate(), 1));
        tripleBuilder.setO(encode(triple.getObject(), 2));
        return tripleBuilder.build();
    }

    RDF_Quad quad(Quad quad, RDF_Quad.Builder quadBuilder) {
        quadBuilder.clear();
        if ( quad.getGraph() != null )
            quadBuilder.setG(encode(quad.getGraph(), 3));
        quadBuilder.setS(encode(quad.getSubject(), 0));
        quadBuilder.setP(encode(quad.getPredicate(), 1));
        quadBuilder.setO(encode(quad.getObject(), 2));
        return quadBuilder.build();
    }

    /** Encode a term in a row position: 0 subject, 1 predicate, 2 object, 3 graph. */
    private RDF_Term encode(Node node, int position) {
        if ( repeat(node, position) )
            return REPEAT;
        return term(node);
    }

    // The table operations are also used by ProtobufStreamWriter, which writes
    // the wire format directly rather than through RDF_Term objects.

    /**
     * Whether the term is the same as the term of the previous row in this
     * position. If not, it becomes the previous term for the next row.
     */
    boolean repeat(Node node, int position) {
        if ( node.equals(previous[position]) )
            return true;
        previous[position] = node;
        return false;
    }

    /** The slot of a term in the term table, or -1. */
    int termSlot(Node node) {
        return termIndex.get(node);
    }

    /** The slot of a datatype in the datatype table, or -1. */
    int datatypeSlot(String datatype) {
        Integer slot = datatypeIndex.get(datatype);
        return ( slot == null ) ? -1 : slot;
    }

    /** Encode a term, using and maintaining the tables. */
    RDF_Term term(Node node) {
        if ( node == null ) {
            termBuilder.clear();
            return ProtobufConvert.toProtobuf(node, PMAP0, termBuilder, false);
        }
        if ( node.isURI() || node.isBlank() ) {
            int slot = termIndex.get(node);
            if ( slot >= 0 )
                return reference(slot);
            termBuilder.clear();
            RDF_Term term = ProtobufConvert.toProtobuf(node, PMAP0, termBuilder, false);
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
            // The terms inside a triple term use the tables but not RDF_REPEAT.
            Triple triple = node.getTriple();
            RDF_Term s = term(triple.getSubject());
            RDF_Term p = term(triple.getPredicate());
            RDF_Term o = term(triple.getObject());
            RDF_Triple tripleTerm = RDF_Triple.newBuilder().setS(s).setP(p).setO(o).build();
            return RDF_Term.newBuilder().setTripleTerm(tripleTerm).build();
        }
        // Variables and ANY.
        termBuilder.clear();
        return ProtobufConvert.toProtobuf(node, PMAP0, termBuilder, false);
    }

    private RDF_Term reference(int slot) {
        RDF_Term ref = references[slot];
        if ( ref == null ) {
            ref = RDF_Term.newBuilder().setTermRef(slot).build();
            references[slot] = ref;
        }
        return ref;
    }

    /** Add a term written in full to the term table. */
    void enterTerm(Node node) {
        int slot = nextTerm;
        Node evicted = termTable[slot];
        if ( evicted != null )
            termIndex.remove(evicted);
        termTable[slot] = node;
        termIndex.put(node, slot);
        nextTerm = ( slot+1 == termTable.length ) ? 0 : slot+1;
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
