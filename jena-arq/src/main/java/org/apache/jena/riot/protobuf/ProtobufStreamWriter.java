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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;
import com.google.protobuf.WireFormat;
import org.apache.jena.atlas.io.IO;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.datatypes.xsd.impl.RDFDirLangString;
import org.apache.jena.datatypes.xsd.impl.RDFLangString;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.TextDirection;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.protobuf.wire.PB_RDF.*;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapFactory;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;

/**
 * Write a length-delimited RDF Protobuf stream straight from {@link StreamRDF}
 * events to the bytes.
 * <p>
 * This writes the same wire format as the generated {@code PB_RDF} classes,
 * version 1 or version 2, but the common cases, IRIs, blank nodes, literals,
 * references and repeats, are written field by field from the {@link Node}
 * instead of first building an {@code RDF_StreamRow} object tree for every row.
 * Rarer terms (triple terms, variables, value-encoded literals) and the
 * non-triple rows go through the wire objects.
 * <p>
 * {@link ProtobufStreamReader} is the matching reader.
 */
final class ProtobufStreamWriter implements StreamRDF {

    private static final int LEN = WireFormat.WIRETYPE_LENGTH_DELIMITED;

    // Term plan kinds.
    private static final int REPEAT = 1;
    private static final int REF = 2;
    private static final int IRI = 3;
    private static final int BNODE = 4;
    private static final int LITERAL = 5;
    private static final int MESSAGE = 6;
    private static final int NSIRI = 7;

    // Literal kinds, by RDF_Literal field number.
    private static final int LIT_LANGTAG = 2;
    private static final int LIT_DATATYPE = 3;
    private static final int LIT_LANGDIR = 5;
    private static final int LIT_DATATYPEREF = 6;
    private static final int LIT_SIMPLE = 9;

    /** What to write for one term, decided once and used for the size and for the bytes. */
    private static final class TermPlan {
        int kind;
        String string;      // IRI, blank node label, literal lexical form or local name.
        int literalKind;
        String literalString;  // Language tag, datatype IRI, language-direction or namespace.
        int slot;           // Term reference, namespace reference or datatype reference.
        MessageLite message;
        int innerSize;      // Size of the RDF_IRI, RDF_BNode, RDF_NsIRI or RDF_Literal inside the term.
        int size;           // Size of the RDF_Term.
    }

    private static final RDF_Term UNDEF = RDF_Term.newBuilder().setUndefined(RDF_UNDEF.getDefaultInstance()).build();

    private static final PrefixMap PMAP0 = PrefixMapFactory.emptyPrefixMap();

    private final OutputStream outputStream;
    private final CodedOutputStream out;
    private final boolean encodeValues;
    // Frames: rows are written as the repeated field of RDF_Stream messages, each
    // of up to batchSize rows, and each finished frame is handed to the consumer.
    private final FrameBuffer frameBuffer;
    private final Consumer<ByteBuffer> frames;
    private final int batchSize;
    private int frameRows = 0;
    // Version 2 tables, or null for version 1 where every term is written in full.
    private final ProtobufEncoderV2 tables;
    private boolean headerSent = false;
    private final TermPlan[] plans = { new TermPlan(), new TermPlan(), new TermPlan(), new TermPlan() };
    private final RDF_Term.Builder termBuilder = RDF_Term.newBuilder();

    ProtobufStreamWriter(OutputStream outputStream, boolean encodeValues, ProtobufEncoderV2 tables) {
        this.outputStream = outputStream;
        this.out = PBufRDF.delimitedOutput(outputStream);
        this.encodeValues = encodeValues;
        this.tables = tables;
        this.frameBuffer = null;
        this.frames = null;
        this.batchSize = 0;
    }

    /** A writer of frames: each is the bytes of an {@code RDF_Stream} message of up to {@code batchSize} rows. */
    ProtobufStreamWriter(Consumer<ByteBuffer> frames, int batchSize, boolean encodeValues, ProtobufEncoderV2 tables) {
        this.frameBuffer = new FrameBuffer();
        this.outputStream = frameBuffer;
        this.out = PBufRDF.delimitedOutput(frameBuffer);
        this.encodeValues = encodeValues;
        this.tables = tables;
        this.frames = frames;
        this.batchSize = batchSize;
    }

    @Override
    public void start() {
        header();
    }

    @Override
    public void finish() {
        if ( frames != null ) {
            if ( frameRows > 0 )
                emitFrame();
            return;
        }
        PBufRDF.flush(out);
        IO.flush(outputStream);
    }

    /** Count a row of a frame and hand the frame on when it is full. */
    private void rowWritten() {
        if ( frames != null && ++frameRows >= batchSize )
            emitFrame();
    }

    private void emitFrame() {
        PBufRDF.flush(out);
        frameRows = 0;
        frames.accept(frameBuffer.take());
    }

    // A version 2 stream starts with a header row. It is written before the
    // first row, not only in start(), so the stream is right even if start()
    // is not called.
    private void header() {
        if ( tables == null || headerSent )
            return;
        headerSent = true;
        row(5, tables.header());
    }

    @Override
    public void base(String base) {
        header();
        row(4, RDF_IRI.newBuilder().setIri(base).build());
    }

    @Override
    public void prefix(String prefix, String iri) {
        header();
        row(1, RDF_PrefixDecl.newBuilder().setPrefix(prefix).setUri(iri).build());
    }

    @Override
    public void version(String version) {}

    /** Write a row whose one field is a wire object. */
    private void row(int field, MessageLite message) {
        try {
            int size = message.getSerializedSize();
            rowStart(messageSize(size));
            out.writeTag(field, LEN);
            out.writeUInt32NoTag(size);
            message.writeTo(out);
        } catch (IOException ex) { IO.exception(ex); }
        rowWritten();
    }

    /**
     * Start a row of the given size: its length for a delimited stream, or the tag of the
     * {@code rows} field of {@code RDF_Stream} and the length for a frame.
     */
    private void rowStart(int size) throws IOException {
        if ( frames != null )
            out.writeTag(1, LEN);
        out.writeUInt32NoTag(size);
    }

    @Override
    public void triple(Triple triple) {
        header();
        try {
            int size = plan(triple.getSubject(), 0)
                     + plan(triple.getPredicate(), 1)
                     + plan(triple.getObject(), 2);
            rowStart(messageSize(size));
            out.writeTag(2, LEN);
            out.writeUInt32NoTag(size);
            term(1, 4, plans[0]);
            term(2, 5, plans[1]);
            term(3, 6, plans[2]);
        } catch (IOException ex) { IO.exception(ex); }
        rowWritten();
    }

    @Override
    public void quad(Quad quad) {
        header();
        try {
            // Version 1 leaves out a null graph; version 2 writes it as RDF_UNDEF,
            // because a term left out means the term of the previous row.
            boolean withGraph = ( quad.getGraph() != null || tables != null );
            int size = plan(quad.getSubject(), 0)
                     + plan(quad.getPredicate(), 1)
                     + plan(quad.getObject(), 2);
            // Plan the graph after the subject, predicate and object; the order
            // terms enter the table must match the order the reader meets them.
            // The reader reads fields in wire order, which is subject, predicate,
            // object, graph, so write them in that order too.
            if ( withGraph )
                size += plan(quad.getGraph(), 3);
            rowStart(messageSize(size));
            out.writeTag(3, LEN);
            out.writeUInt32NoTag(size);
            term(1, 5, plans[0]);
            term(2, 6, plans[1]);
            term(3, 7, plans[2]);
            if ( withGraph )
                term(4, 8, plans[3]);
        } catch (IOException ex) { IO.exception(ex); }
        rowWritten();
    }

    /** Size of a length-delimited field holding a message of the given size: tag, length, bytes. */
    private static int messageSize(int size) {
        return 1 + CodedOutputStream.computeUInt32SizeNoTag(size) + size;
    }

    /**
     * Decide how to write a term in a row position and return the size of the
     * term as a field of the triple or quad: tag, length and the RDF_Term bytes,
     * or the tag and value of a reference, or nothing for a term left out.
     */
    private int plan(Node node, int position) {
        TermPlan plan = plans[position];
        plan.message = null;
        if ( node == null )
            // The null graph of a quad (version 2: version 1 does not get here).
            return planMessage(plan, UNDEF);
        if ( tables != null && tables.repeat(node, position) ) {
            plan.kind = REPEAT;
            plan.size = 0;
            return 0;
        }
        if ( node.isURI() || node.isBlank() ) {
            if ( tables != null ) {
                int ref = tables.termRef(node);
                if ( ref > 0 ) {
                    plan.kind = REF;
                    plan.slot = ref;
                    // The reference is a field of the triple or quad: one byte of tag.
                    plan.size = 1 + CodedOutputStream.computeUInt32SizeNoTag(ref);
                    return plan.size;
                }
                tables.enterTerm(node);
                if ( node.isURI() ) {
                    String iri = node.getURI();
                    int nsEnd = ProtobufEncoderV2.namespaceEnd(iri);
                    if ( nsEnd > 0 ) {
                        plan.kind = NSIRI;
                        plan.string = iri.substring(nsEnd);
                        String namespace = iri.substring(0, nsEnd);
                        int nsRef = tables.namespaceRef(namespace);
                        int inner;
                        if ( nsRef > 0 ) {
                            plan.slot = nsRef;
                            plan.literalString = null;
                            inner = CodedOutputStream.computeUInt32Size(2, nsRef);
                        } else {
                            tables.enterNamespace(namespace);
                            plan.slot = -1;
                            plan.literalString = namespace;
                            inner = CodedOutputStream.computeStringSize(1, namespace);
                        }
                        if ( !plan.string.isEmpty() )
                            inner += CodedOutputStream.computeStringSize(3, plan.string);
                        plan.innerSize = inner;
                        plan.size = messageSize(inner);
                        return messageSize(plan.size);
                    }
                }
            }
            plan.kind = node.isURI() ? IRI : BNODE;
            plan.string = node.isURI() ? node.getURI() : node.getBlankNodeLabel();
            plan.innerSize = stringFieldSize(plan.string);
            plan.size = messageSize(plan.innerSize);
            return messageSize(plan.size);
        }
        if ( node.isLiteral() ) {
            if ( encodeValues ) {
                termBuilder.clear();
                RDF_Term value = ProtobufConvert.toProtobufValue(node, termBuilder);
                if ( value != null )
                    return planMessage(plan, value);
            }
            plan.kind = LITERAL;
            plan.string = node.getLiteralLexicalForm();
            int inner = stringFieldSize(plan.string);
            RDFDatatype dt = node.getLiteralDatatype();
            String lang = node.getLiteralLanguage();
            if ( dt.equals(XSDDatatype.XSDstring) || dt.equals(RDFLangString.rdfLangString) || dt.equals(RDFDirLangString.rdfDirLangString) ) {
                TextDirection baseDir = node.getLiteralBaseDirection();
                if ( lang.isEmpty() ) {
                    plan.literalKind = LIT_SIMPLE;
                    inner += 2;
                } else if ( baseDir == null ) {
                    plan.literalKind = LIT_LANGTAG;
                    plan.literalString = lang;
                    inner += CodedOutputStream.computeStringSize(LIT_LANGTAG, lang);
                } else {
                    plan.literalKind = LIT_LANGDIR;
                    plan.literalString = lang+"--"+baseDir.direction();
                    inner += CodedOutputStream.computeStringSize(LIT_LANGDIR, plan.literalString);
                }
            } else {
                String datatype = dt.getURI();
                int slot = ( tables == null ) ? -1 : tables.datatypeSlot(datatype);
                if ( slot >= 0 ) {
                    plan.literalKind = LIT_DATATYPEREF;
                    plan.slot = slot;
                    inner += CodedOutputStream.computeUInt32Size(LIT_DATATYPEREF, slot);
                } else {
                    if ( tables != null )
                        tables.enterDatatype(datatype);
                    plan.literalKind = LIT_DATATYPE;
                    plan.literalString = datatype;
                    inner += CodedOutputStream.computeStringSize(LIT_DATATYPE, datatype);
                }
            }
            plan.innerSize = inner;
            plan.size = messageSize(inner);
            return messageSize(plan.size);
        }
        // Triple terms, variables, ANY: build the wire object.
        RDF_Term term;
        if ( tables != null )
            term = tables.term(node);
        else {
            termBuilder.clear();
            term = ProtobufConvert.toProtobuf(node, PMAP0, termBuilder, encodeValues);
        }
        return planMessage(plan, term);
    }

    private static int planMessage(TermPlan plan, RDF_Term term) {
        plan.kind = MESSAGE;
        plan.message = term;
        plan.size = term.getSerializedSize();
        return messageSize(plan.size);
    }

    /** Size of a string as field 1 of RDF_IRI, RDF_BNode or RDF_Literal: not written when empty, as in proto3. */
    private static int stringFieldSize(String value) {
        return value.isEmpty() ? 0 : CodedOutputStream.computeStringSize(1, value);
    }

    /**
     * Write a planned term as a field of a triple or quad: the RDF_Term field, the
     * reference field, or nothing for a term left out.
     */
    private void term(int field, int refField, TermPlan plan) throws IOException {
        if ( plan.kind == REPEAT )
            return;
        if ( plan.kind == REF ) {
            out.writeUInt32(refField, plan.slot);
            return;
        }
        out.writeTag(field, LEN);
        out.writeUInt32NoTag(plan.size);
        switch (plan.kind) {
            case NSIRI -> {
                out.writeTag(11, LEN);
                out.writeUInt32NoTag(plan.innerSize);
                if ( plan.slot > 0 )
                    out.writeUInt32(2, plan.slot);
                else
                    out.writeString(1, plan.literalString);
                if ( !plan.string.isEmpty() )
                    out.writeString(3, plan.string);
            }
            case IRI, BNODE -> {
                out.writeTag(plan.kind == IRI ? 1 : 2, LEN);
                out.writeUInt32NoTag(plan.innerSize);
                if ( !plan.string.isEmpty() )
                    out.writeString(1, plan.string);
            }
            case LITERAL -> {
                out.writeTag(3, LEN);
                out.writeUInt32NoTag(plan.innerSize);
                if ( !plan.string.isEmpty() )
                    out.writeString(1, plan.string);
                switch (plan.literalKind) {
                    case LIT_SIMPLE -> out.writeBool(LIT_SIMPLE, true);
                    case LIT_LANGTAG, LIT_DATATYPE, LIT_LANGDIR -> out.writeString(plan.literalKind, plan.literalString);
                    case LIT_DATATYPEREF -> out.writeUInt32(LIT_DATATYPEREF, plan.slot);
                    default -> throw new RiotProtobufException("Bad literal kind");
                }
            }
            case MESSAGE -> plan.message.writeTo(out);
            default -> throw new RiotProtobufException("Bad term plan");
        }
    }

    /**
     * Bytes of the frame being written. {@link #take()} hands the filled part over as a
     * buffer that the consumer owns and starts a new one.
     */
    private static final class FrameBuffer extends OutputStream {
        private static final int INITIAL = 8 * 1024;
        private byte[] bytes = new byte[INITIAL];
        private int count = 0;

        @Override
        public void write(int b) {
            ensure(1);
            bytes[count++] = (byte)b;
        }

        @Override
        public void write(byte[] b, int off, int len) {
            ensure(len);
            System.arraycopy(b, off, bytes, count, len);
            count += len;
        }

        private void ensure(int more) {
            if ( count + more > bytes.length )
                bytes = java.util.Arrays.copyOf(bytes, Math.max(bytes.length * 2, count + more));
        }

        ByteBuffer take() {
            ByteBuffer frame = ByteBuffer.wrap(bytes, 0, count);
            // The next frame is likely about the same size.
            bytes = new byte[Math.max(INITIAL, count)];
            count = 0;
            return frame;
        }
    }
}
