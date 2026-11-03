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

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.lib.Cache;
import org.apache.jena.atlas.lib.CacheFactory;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.FactoryRDFCaching;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.RiotLib;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;

/**
 * Read a length-delimited RDF Protobuf stream straight from the bytes into a
 * {@link StreamRDF}.
 * <p>
 * This reads the same wire format as the generated {@code PB_RDF} classes,
 * version 1 and version 2, but builds {@link Node}s directly from the fields as
 * they are read instead of first building an {@code RDF_StreamRow} object tree
 * for every row. For a stream of millions of rows, those short-lived objects are
 * most of the cost of reading. Fields are matched by tag, in any order, and
 * unknown fields are skipped, as the generated parser does.
 * <p>
 * {@link Protobuf2StreamRDF} does the same conversion from wire objects, for
 * code that already has them.
 */
final class ProtobufStreamReader {

    // Tags: (field number << 3) | wire type. Written out so they are constants a switch can use.
    private static final int LEN = WireFormat.WIRETYPE_LENGTH_DELIMITED;
    private static final int VARINT = WireFormat.WIRETYPE_VARINT;
    private static final int FIXED64 = WireFormat.WIRETYPE_FIXED64;

    // RDF_StreamRow
    private static final int ROW_PREFIXDECL = (1 << 3) | LEN;
    private static final int ROW_TRIPLE     = (2 << 3) | LEN;
    private static final int ROW_QUAD       = (3 << 3) | LEN;
    private static final int ROW_BASE       = (4 << 3) | LEN;
    private static final int ROW_HEADER     = (5 << 3) | LEN;
    // RDF_Triple and RDF_Quad
    private static final int TRIPLE_S = (1 << 3) | LEN;
    private static final int TRIPLE_P = (2 << 3) | LEN;
    private static final int TRIPLE_O = (3 << 3) | LEN;
    private static final int QUAD_G   = (4 << 3) | LEN;
    // RDF_Term
    private static final int TERM_IRI        = (1 << 3) | LEN;
    private static final int TERM_BNODE      = (2 << 3) | LEN;
    private static final int TERM_LITERAL    = (3 << 3) | LEN;
    private static final int TERM_PREFIXNAME = (4 << 3) | LEN;
    private static final int TERM_VARIABLE   = (5 << 3) | LEN;
    private static final int TERM_TRIPLETERM = (6 << 3) | LEN;
    private static final int TERM_ANY        = (7 << 3) | LEN;
    private static final int TERM_UNDEFINED  = (8 << 3) | LEN;
    private static final int TERM_REPEAT     = (9 << 3) | LEN;
    private static final int TERM_REF        = (10 << 3) | VARINT;
    private static final int TERM_VALINTEGER = (20 << 3) | VARINT;
    private static final int TERM_VALDOUBLE  = (21 << 3) | FIXED64;
    private static final int TERM_VALDECIMAL = (22 << 3) | LEN;
    // RDF_IRI, RDF_BNode, RDF_Var: one string field.
    private static final int STRING_1 = (1 << 3) | LEN;
    // RDF_PrefixName and RDF_PrefixDecl: two string fields.
    private static final int STRING_2 = (2 << 3) | LEN;
    // RDF_Literal
    private static final int LITERAL_LEX         = (1 << 3) | LEN;
    private static final int LITERAL_LANGTAG     = (2 << 3) | LEN;
    private static final int LITERAL_DATATYPE    = (3 << 3) | LEN;
    private static final int LITERAL_DTPREFIX    = (4 << 3) | LEN;
    private static final int LITERAL_LANGDIR     = (5 << 3) | LEN;
    private static final int LITERAL_DATATYPEREF = (6 << 3) | VARINT;
    private static final int LITERAL_SIMPLE      = (9 << 3) | VARINT;
    // RDF_Decimal
    private static final int DECIMAL_VALUE = (1 << 3) | VARINT;
    private static final int DECIMAL_SCALE = (2 << 3) | VARINT;
    // RDF_Header
    private static final int HEADER_VERSION           = (1 << 3) | VARINT;
    private static final int HEADER_TERMTABLESIZE     = (2 << 3) | VARINT;
    private static final int HEADER_DATATYPETABLESIZE = (3 << 3) | VARINT;

    private static final int NO_POSITION = -1;

    // RDF_Stream
    private static final int STREAM_ROW = (1 << 3) | LEN;

    private CodedInputStream in;
    private final StreamRDF dest;
    private final PrefixMap pmap;
    // Version 1: IRIs are interned through a cache, as Protobuf2StreamRDF does.
    private final Cache<String, Node> uriCache = CacheFactory.createSimpleCache(FactoryRDFCaching.DftNodeCacheSize);
    // Version 2: the tables, once a header has been read.
    private final ProtobufDecoderV2 tables;
    private boolean version2 = false;

    ProtobufStreamReader(CodedInputStream in, PrefixMap pmap, StreamRDF dest) {
        this.in = in;
        this.pmap = pmap;
        this.dest = dest;
        this.tables = new ProtobufDecoderV2(pmap);
    }

    /** A reader of frames; see {@link #readFrame(ByteBuffer)}. */
    ProtobufStreamReader(PrefixMap pmap, StreamRDF dest) {
        this(null, pmap, dest);
    }

    // Nested messages are read up to their end position rather than by pushing
    // a limit on the CodedInputStream for each one. Pushing and popping limits,
    // and the isAtEnd() check against a limit, show up in profiles when done
    // several times per row. The end position is checked after each message.

    /** Read rows until the end of the input. */
    void read() {
        try {
            while ( !in.isAtEnd() ) {
                // Keep the byte counter from growing without bound on a long stream.
                in.resetSizeCounter();
                row(end());
            }
        } catch (IOException ex) { IO.exception(ex); }
    }

    /**
     * Read one frame: the bytes of an {@code RDF_Stream} message, that is a sequence of
     * {@code rows} fields. The tables, prefixes and node cache carry over from one frame to
     * the next, so the frames of one stream must be read in order by one reader.
     */
    void readFrame(ByteBuffer frame) {
        in = CodedInputStream.newInstance(frame);
        try {
            while ( !in.isAtEnd() ) {
                int tag = in.readTag();
                if ( tag == STREAM_ROW )
                    row(end());
                else
                    in.skipField(tag);
            }
        } catch (IOException ex) { IO.exception(ex); }
    }

    /** Read the length of a nested message and return the position of its end. */
    private int end() throws IOException {
        int length = in.readRawVarint32();
        return in.getTotalBytesRead() + length;
    }

    private void checkEnd(int end) {
        if ( in.getTotalBytesRead() != end )
            throw new RiotProtobufException("Malformed message: read past its length");
    }

    private void row(int end) throws IOException {
        while ( in.getTotalBytesRead() < end ) {
            int tag = in.readTag();
            switch (tag) {
                case ROW_TRIPLE -> dest.triple(triple(end()));
                case ROW_QUAD -> dest.quad(quad(end()));
                case ROW_PREFIXDECL -> prefixDecl(end());
                case ROW_BASE -> dest.base(string1(end()));
                case ROW_HEADER -> header(end());
                default -> in.skipField(tag);
            }
        }
        checkEnd(end);
    }

    private void header(int end) throws IOException {
        int version = 0;
        int termTableSize = 0;
        int datatypeTableSize = 0;
        while ( in.getTotalBytesRead() < end ) {
            int tag = in.readTag();
            switch (tag) {
                case HEADER_VERSION -> version = in.readUInt32();
                case HEADER_TERMTABLESIZE -> termTableSize = in.readUInt32();
                case HEADER_DATATYPETABLESIZE -> datatypeTableSize = in.readUInt32();
                default -> in.skipField(tag);
            }
        }
        checkEnd(end);
        tables.start(version, termTableSize, datatypeTableSize);
        version2 = true;
    }

    private void prefixDecl(int end) throws IOException {
        String prefix = "";
        String uri = "";
        while ( in.getTotalBytesRead() < end ) {
            int tag = in.readTag();
            switch (tag) {
                case STRING_1 -> prefix = in.readString();
                case STRING_2 -> uri = in.readString();
                default -> in.skipField(tag);
            }
        }
        checkEnd(end);
        pmap.add(prefix, uri);
        dest.prefix(prefix, uri);
    }

    private Triple triple(int end) throws IOException {
        Node s = null, p = null, o = null;
        while ( in.getTotalBytesRead() < end ) {
            int tag = in.readTag();
            switch (tag) {
                case TRIPLE_S -> s = term(0);
                case TRIPLE_P -> p = term(1);
                case TRIPLE_O -> o = term(2);
                default -> in.skipField(tag);
            }
        }
        checkEnd(end);
        if ( s == null || p == null || o == null )
            throw new RiotProtobufException("RDF_Triple with a missing term");
        return Triple.create(s, p, o);
    }

    private Quad quad(int end) throws IOException {
        Node g = null, s = null, p = null, o = null;
        while ( in.getTotalBytesRead() < end ) {
            int tag = in.readTag();
            switch (tag) {
                case QUAD_G   -> g = term(3);
                case TRIPLE_S -> s = term(0);
                case TRIPLE_P -> p = term(1);
                case TRIPLE_O -> o = term(2);
                default -> in.skipField(tag);
            }
        }
        checkEnd(end);
        if ( s == null || p == null || o == null )
            throw new RiotProtobufException("RDF_Quad with a missing term");
        return Quad.create(g, s, p, o);
    }

    /**
     * Read a length-delimited RDF_Term. The position (0 subject, 1 predicate,
     * 2 object, 3 graph) is for RDF_REPEAT; inside a triple term there is none.
     */
    private Node term(int position) throws IOException {
        int end = end();
        Node node = null;
        boolean repeat = false;
        while ( in.getTotalBytesRead() < end ) {
            int tag = in.readTag();
            switch (tag) {
                case TERM_IRI -> node = iri(string1(end()));
                case TERM_BNODE -> {
                    node = NodeFactory.createBlankNode(string1(end()));
                    if ( version2 )
                        tables.enterTerm(node);
                }
                case TERM_LITERAL -> node = literal(end());
                case TERM_PREFIXNAME -> node = iri(prefixName(end()));
                case TERM_VARIABLE -> node = Var.alloc(string1(end()));
                case TERM_TRIPLETERM -> node = NodeFactory.createTripleTerm(tripleTerm(end()));
                case TERM_ANY -> {
                    skipMessage();
                    node = Node.ANY;
                }
                case TERM_UNDEFINED -> {
                    skipMessage();
                    node = null;
                }
                case TERM_REPEAT -> {
                    skipMessage();
                    if ( !version2 )
                        throw new RiotProtobufException("RDF_REPEAT outside a version 2 stream");
                    if ( position == NO_POSITION )
                        throw new RiotProtobufException("RDF_REPEAT inside a triple term");
                    node = tables.repeat(position);
                    repeat = true;
                }
                case TERM_REF -> {
                    int slot = in.readUInt32();
                    if ( !version2 )
                        throw new RiotProtobufException("Term reference outside a version 2 stream");
                    node = tables.termRef(slot);
                }
                case TERM_VALINTEGER -> node = NodeFactory.createLiteralDT(Long.toString(in.readSInt64()), XSDDatatype.XSDinteger);
                case TERM_VALDOUBLE -> node = NodeFactory.createLiteralDT(Double.toString(in.readDouble()), XSDDatatype.XSDdouble);
                case TERM_VALDECIMAL -> node = decimal(end());
                default -> in.skipField(tag);
            }
        }
        checkEnd(end);
        if ( position != NO_POSITION && !repeat )
            tables.previous(position, node);
        return node;
    }

    private Node iri(String iriStr) {
        if ( version2 ) {
            Node node = RiotLib.createIRIorBNode(iriStr);
            tables.enterTerm(node);
            return node;
        }
        return uriCache.get(iriStr, RiotLib::createIRIorBNode);
    }

    private Triple tripleTerm(int end) throws IOException {
        Node s = null, p = null, o = null;
        while ( in.getTotalBytesRead() < end ) {
            int tag = in.readTag();
            switch (tag) {
                case TRIPLE_S -> s = term(NO_POSITION);
                case TRIPLE_P -> p = term(NO_POSITION);
                case TRIPLE_O -> o = term(NO_POSITION);
                default -> in.skipField(tag);
            }
        }
        checkEnd(end);
        if ( s == null || p == null || o == null )
            throw new RiotProtobufException("Triple term with a missing term");
        return Triple.create(s, p, o);
    }

    private Node literal(int end) throws IOException {
        String lex = "";
        // Which of the oneof fields was present, by tag, and its value.
        int kind = 0;
        String kindValue = null;
        int datatypeRef = 0;
        while ( in.getTotalBytesRead() < end ) {
            int tag = in.readTag();
            switch (tag) {
                case LITERAL_LEX -> lex = in.readString();
                case LITERAL_SIMPLE -> { in.readBool(); kind = tag; }
                case LITERAL_LANGTAG, LITERAL_DATATYPE, LITERAL_LANGDIR -> { kindValue = in.readString(); kind = tag; }
                case LITERAL_DTPREFIX -> { kindValue = prefixName(end()); kind = tag; }
                case LITERAL_DATATYPEREF -> { datatypeRef = in.readUInt32(); kind = tag; }
                default -> in.skipField(tag);
            }
        }
        checkEnd(end);
        switch (kind) {
            case LITERAL_SIMPLE :
                return NodeFactory.createLiteralString(lex);
            case LITERAL_LANGTAG :
                return NodeFactory.createLiteralLang(lex, kindValue);
            case LITERAL_LANGDIR : {
                int idx = kindValue.indexOf("--");
                if ( idx < 0 )
                    throw new RiotProtobufException("Bad language-base direction value: "+kindValue);
                return NodeFactory.createLiteralDirLang(lex, kindValue.substring(0, idx), kindValue.substring(idx+2));
            }
            case LITERAL_DATATYPE : {
                RDFDatatype dt = NodeFactory.getType(kindValue);
                if ( version2 )
                    tables.enterDatatype(dt);
                return NodeFactory.createLiteralDT(lex, dt);
            }
            case LITERAL_DTPREFIX :
                return NodeFactory.createLiteralDT(lex, NodeFactory.getType(kindValue));
            case LITERAL_DATATYPEREF :
                if ( !version2 )
                    throw new RiotProtobufException("Datatype reference outside a version 2 stream");
                return NodeFactory.createLiteralDT(lex, tables.datatypeRef(datatypeRef));
            default :
                throw new RiotProtobufException("Literal kind not set.");
        }
    }

    private Node decimal(int end) throws IOException {
        long value = 0;
        int scale = 0;
        while ( in.getTotalBytesRead() < end ) {
            int tag = in.readTag();
            switch (tag) {
                case DECIMAL_VALUE -> value = in.readSInt64();
                case DECIMAL_SCALE -> scale = in.readSInt32();
                default -> in.skipField(tag);
            }
        }
        checkEnd(end);
        return NodeFactory.createLiteralDT(BigDecimal.valueOf(value, scale).toPlainString(), XSDDatatype.XSDdecimal);
    }

    /** A prefix name, expanded with the prefixes declared so far in the stream. */
    private String prefixName(int end) throws IOException {
        String prefix = "";
        String localName = "";
        while ( in.getTotalBytesRead() < end ) {
            int tag = in.readTag();
            switch (tag) {
                case STRING_1 -> prefix = in.readString();
                case STRING_2 -> localName = in.readString();
                default -> in.skipField(tag);
            }
        }
        checkEnd(end);
        String x = pmap.expand(prefix, localName);
        if ( x == null )
            throw new RiotProtobufException("Failed to expand "+prefix+":"+localName);
        return x;
    }

    /** The one string field of RDF_IRI, RDF_BNode and RDF_Var. */
    private String string1(int end) throws IOException {
        String value = "";
        while ( in.getTotalBytesRead() < end ) {
            int tag = in.readTag();
            if ( tag == STRING_1 )
                value = in.readString();
            else
                in.skipField(tag);
        }
        checkEnd(end);
        return value;
    }

    /** Skip a length-delimited message, such as the empty RDF_ANY. */
    private void skipMessage() throws IOException {
        in.skipRawBytes(in.readRawVarint32());
    }
}
