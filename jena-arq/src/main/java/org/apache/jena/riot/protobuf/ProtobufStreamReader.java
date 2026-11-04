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
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;

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
import org.apache.jena.graph.impl.LiteralLabelFactory;
import org.apache.jena.langtagx.LangTagX;
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
 * The input is read in chunks into a byte array and parsed from there, which is
 * the fast path of {@link CodedInputStream}; a row is only parsed once all its
 * bytes are in the array.
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
    private static final int TRIPLE_S    = (1 << 3) | LEN;
    private static final int TRIPLE_P    = (2 << 3) | LEN;
    private static final int TRIPLE_O    = (3 << 3) | LEN;
    private static final int TRIPLE_SREF = (4 << 3) | VARINT;
    private static final int TRIPLE_PREF = (5 << 3) | VARINT;
    private static final int TRIPLE_OREF = (6 << 3) | VARINT;
    private static final int QUAD_G      = (4 << 3) | LEN;
    private static final int QUAD_SREF   = (5 << 3) | VARINT;
    private static final int QUAD_PREF   = (6 << 3) | VARINT;
    private static final int QUAD_OREF   = (7 << 3) | VARINT;
    private static final int QUAD_GREF   = (8 << 3) | VARINT;
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
    private static final int TERM_NSIRI      = (11 << 3) | LEN;
    private static final int TERM_VALINTEGER = (20 << 3) | VARINT;
    private static final int TERM_VALDOUBLE  = (21 << 3) | FIXED64;
    private static final int TERM_VALDECIMAL = (22 << 3) | LEN;
    // RDF_IRI, RDF_BNode, RDF_Var: one string field.
    private static final int STRING_1 = (1 << 3) | LEN;
    // RDF_PrefixName and RDF_PrefixDecl: two string fields.
    private static final int STRING_2 = (2 << 3) | LEN;
    // RDF_NsIRI
    private static final int NSIRI_NAMESPACE    = (1 << 3) | LEN;
    private static final int NSIRI_NAMESPACEREF = (2 << 3) | VARINT;
    private static final int NSIRI_LOCALNAME    = (3 << 3) | LEN;
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
    private static final int HEADER_VERSION            = (1 << 3) | VARINT;
    private static final int HEADER_TERMTABLESIZE      = (2 << 3) | VARINT;
    private static final int HEADER_DATATYPETABLESIZE  = (3 << 3) | VARINT;
    private static final int HEADER_NAMESPACETABLESIZE = (4 << 3) | VARINT;

    private static final int NO_POSITION = -1;

    // RDF_Stream
    private static final int STREAM_ROW = (1 << 3) | LEN;

    // Which terms of a row have been read.
    private static final int SEEN_S = 1;
    private static final int SEEN_P = 2;
    private static final int SEEN_O = 4;
    private static final int SEEN_G = 8;

    /** Chunk of the input read at a time. */
    static final int CHUNK_SIZE = 1024 * 1024;

    private final InputStream input;
    private CodedInputStream in;
    private final StreamRDF dest;
    private final PrefixMap pmap;
    // Version 1: IRIs are interned through a cache, as Protobuf2StreamRDF does.
    private final Cache<String, Node> uriCache = CacheFactory.createSimpleCache(FactoryRDFCaching.DftNodeCacheSize);
    // Version 2: the tables, once a header has been read.
    private final ProtobufDecoderV2 tables;
    private boolean version2 = false;
    // The last language tag read and its formatted form: Jena parses and formats
    // the tag for every literal, and literals with the same tag come together.
    private String lastLangTag = null;
    private String lastLangTagFormatted = null;

    /** A reader of a length-delimited stream; see {@link #read()}. */
    ProtobufStreamReader(InputStream input, PrefixMap pmap, StreamRDF dest) {
        this.input = input;
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

    /**
     * Read rows until the end of the input. The input is read a chunk at a time:
     * the rows wholly within the chunk are parsed, then what is left of the last,
     * partial row is moved to the start and the chunk filled again.
     */
    void read() {
        byte[] buffer = new byte[CHUNK_SIZE];
        int count = 0;          // Bytes in the buffer.
        boolean eof = false;
        in = CodedInputStream.newInstance(buffer, 0, 0);
        try {
            while (true) {
                int pos = in.getTotalBytesRead();
                // The length of the next row: a varint of up to 5 bytes.
                int length = 0;
                int header = 0;
                while (true) {
                    if ( pos + header == count ) {
                        if ( eof ) {
                            if ( header == 0 && pos == count )
                                return;     // Clean end of input.
                            throw new RiotProtobufException("Truncated stream: end of input inside a row length");
                        }
                        break;
                    }
                    byte b = buffer[pos + header];
                    length |= (b & 0x7F) << (7 * header);
                    header++;
                    if ( b >= 0 )
                        break;
                    if ( header == 5 )
                        throw new RiotProtobufException("Malformed row length");
                }
                boolean lengthRead = ( header > 0 && buffer[pos + header - 1] >= 0 );
                int rowEnd = pos + header + length;
                if ( !lengthRead || rowEnd > count ) {
                    if ( eof )
                        throw new RiotProtobufException("Truncated stream: end of input inside a row");
                    // Move the partial row to the start and read more.
                    count -= pos;
                    System.arraycopy(buffer, pos, buffer, 0, count);
                    int needed = header + length;
                    if ( !lengthRead )
                        needed = 5;
                    if ( needed > buffer.length )
                        buffer = Arrays.copyOf(buffer, Math.max(needed, buffer.length * 2));
                    int n = input.read(buffer, count, buffer.length - count);
                    if ( n < 0 )
                        eof = true;
                    else
                        count += n;
                    in = CodedInputStream.newInstance(buffer, 0, count);
                    continue;
                }
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
        int namespaceTableSize = 0;
        while ( in.getTotalBytesRead() < end ) {
            int tag = in.readTag();
            switch (tag) {
                case HEADER_VERSION -> version = in.readUInt32();
                case HEADER_TERMTABLESIZE -> termTableSize = in.readUInt32();
                case HEADER_DATATYPETABLESIZE -> datatypeTableSize = in.readUInt32();
                case HEADER_NAMESPACETABLESIZE -> namespaceTableSize = in.readUInt32();
                default -> in.skipField(tag);
            }
        }
        checkEnd(end);
        tables.start(version, termTableSize, datatypeTableSize, namespaceTableSize);
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
        int seen = 0;
        while ( in.getTotalBytesRead() < end ) {
            int tag = in.readTag();
            switch (tag) {
                case TRIPLE_S -> { s = term(0); seen |= SEEN_S; }
                case TRIPLE_P -> { p = term(1); seen |= SEEN_P; }
                case TRIPLE_O -> { o = term(2); seen |= SEEN_O; }
                case TRIPLE_SREF -> { s = reference(0); seen |= SEEN_S; }
                case TRIPLE_PREF -> { p = reference(1); seen |= SEEN_P; }
                case TRIPLE_OREF -> { o = reference(2); seen |= SEEN_O; }
                default -> in.skipField(tag);
            }
        }
        checkEnd(end);
        if ( seen != (SEEN_S | SEEN_P | SEEN_O) ) {
            if ( !version2 )
                throw new RiotProtobufException("RDF_Triple with a missing term");
            if ( (seen & SEEN_S) == 0 ) s = tables.repeat(0);
            if ( (seen & SEEN_P) == 0 ) p = tables.repeat(1);
            if ( (seen & SEEN_O) == 0 ) o = tables.repeat(2);
        }
        return Triple.create(s, p, o);
    }

    private Quad quad(int end) throws IOException {
        Node g = null, s = null, p = null, o = null;
        int seen = 0;
        while ( in.getTotalBytesRead() < end ) {
            int tag = in.readTag();
            switch (tag) {
                case TRIPLE_S -> { s = term(0); seen |= SEEN_S; }
                case TRIPLE_P -> { p = term(1); seen |= SEEN_P; }
                case TRIPLE_O -> { o = term(2); seen |= SEEN_O; }
                case QUAD_G   -> { g = term(3); seen |= SEEN_G; }
                case QUAD_SREF -> { s = reference(0); seen |= SEEN_S; }
                case QUAD_PREF -> { p = reference(1); seen |= SEEN_P; }
                case QUAD_OREF -> { o = reference(2); seen |= SEEN_O; }
                case QUAD_GREF -> { g = reference(3); seen |= SEEN_G; }
                default -> in.skipField(tag);
            }
        }
        checkEnd(end);
        if ( seen != (SEEN_S | SEEN_P | SEEN_O | SEEN_G) ) {
            if ( !version2 ) {
                // Version 1: a quad without a graph is in the default graph.
                if ( (seen & (SEEN_S | SEEN_P | SEEN_O)) != (SEEN_S | SEEN_P | SEEN_O) )
                    throw new RiotProtobufException("RDF_Quad with a missing term");
            } else {
                if ( (seen & SEEN_S) == 0 ) s = tables.repeat(0);
                if ( (seen & SEEN_P) == 0 ) p = tables.repeat(1);
                if ( (seen & SEEN_O) == 0 ) o = tables.repeat(2);
                if ( (seen & SEEN_G) == 0 ) g = tables.repeat(3);
            }
        }
        return Quad.create(g, s, p, o);
    }

    /** Read a term reference field of a triple or quad in a row position. */
    private Node reference(int position) throws IOException {
        int distance = in.readUInt32();
        if ( !version2 )
            throw new RiotProtobufException("Term reference outside a version 2 stream");
        Node node = tables.termRef(distance);
        if ( position != NO_POSITION )
            tables.previous(position, node);
        return node;
    }

    /**
     * Read a length-delimited RDF_Term. The position (0 subject, 1 predicate,
     * 2 object, 3 graph) is for the term of the previous row; inside a triple
     * term there is none.
     */
    private Node term(int position) throws IOException {
        int end = end();
        Node node = null;
        while ( in.getTotalBytesRead() < end ) {
            int tag = in.readTag();
            switch (tag) {
                case TERM_IRI -> node = iri(string1(end()));
                case TERM_NSIRI -> node = nsIri(end());
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
                case TERM_REPEAT -> throw new RiotProtobufException("RDF_REPEAT is not supported");
                case TERM_VALINTEGER -> node = NodeFactory.createLiteralDT(Long.toString(in.readSInt64()), XSDDatatype.XSDinteger);
                case TERM_VALDOUBLE -> node = NodeFactory.createLiteralDT(Double.toString(in.readDouble()), XSDDatatype.XSDdouble);
                case TERM_VALDECIMAL -> node = decimal(end());
                default -> in.skipField(tag);
            }
        }
        checkEnd(end);
        if ( position != NO_POSITION && version2 )
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

    /** An IRI as namespace and local name (version 2). */
    private Node nsIri(int end) throws IOException {
        if ( !version2 )
            throw new RiotProtobufException("RDF_NsIRI outside a version 2 stream");
        String namespace = null;
        String localName = "";
        while ( in.getTotalBytesRead() < end ) {
            int tag = in.readTag();
            switch (tag) {
                case NSIRI_NAMESPACE -> {
                    namespace = in.readString();
                    tables.enterNamespace(namespace);
                }
                case NSIRI_NAMESPACEREF -> namespace = tables.namespaceRef(in.readUInt32());
                case NSIRI_LOCALNAME -> localName = in.readString();
                default -> in.skipField(tag);
            }
        }
        checkEnd(end);
        if ( namespace == null )
            throw new RiotProtobufException("RDF_NsIRI without a namespace");
        Node node = tables.iri(namespace, localName);
        tables.enterTerm(node);
        return node;
    }

    private Triple tripleTerm(int end) throws IOException {
        Node s = null, p = null, o = null;
        while ( in.getTotalBytesRead() < end ) {
            int tag = in.readTag();
            switch (tag) {
                case TRIPLE_S -> s = term(NO_POSITION);
                case TRIPLE_P -> p = term(NO_POSITION);
                case TRIPLE_O -> o = term(NO_POSITION);
                case TRIPLE_SREF -> s = reference(NO_POSITION);
                case TRIPLE_PREF -> p = reference(NO_POSITION);
                case TRIPLE_OREF -> o = reference(NO_POSITION);
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
                return langLiteral(lex, kindValue);
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

    /** A literal with a language tag, formatted once per run of the same tag. */
    private Node langLiteral(String lex, String lang) {
        if ( lang.isEmpty() )
            return NodeFactory.createLiteralLang(lex, lang);
        if ( !lang.equals(lastLangTag) ) {
            lastLangTagFormatted = LangTagX.formatLanguageTag(lang);
            lastLangTag = lang;
        }
        // The same literal as NodeFactory.createLiteralLang(lex, lang) makes.
        return NodeFactory.createLiteral(LiteralLabelFactory.createLang(lex, lastLangTagFormatted));
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
