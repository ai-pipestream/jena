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
import java.io.OutputStream;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.lib.InternalErrorException;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Quad;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_StreamRow;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Term;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Triple;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapFactory;
import org.apache.jena.sparql.core.Quad;

/** Package support functions */
class PBufRDF {

    private static PrefixMap PMAP0 = PrefixMapFactory.emptyPrefixMap();

    /**
     * Buffer size for the shared {@link CodedInputStream} and {@link CodedOutputStream}
     * used for length-delimited streams. Each row is small, so this comfortably
     * holds many rows per refill.
     */
    private static final int BUFFER_SIZE = 32*1024;

    /**
     * Create a {@link CodedInputStream} for reading a length-delimited stream of
     * messages with {@link #readDelimited}.
     * <p>
     * The one {@code CodedInputStream} is used for the whole stream.
     * {@code MessageLite.parseDelimitedFrom(InputStream)} creates a new
     * {@code CodedInputStream}, with its own buffer, for every message, which
     * is a large part of the per-row cost when reading millions of rows.
     */
    static CodedInputStream delimitedInput(InputStream input) {
        return CodedInputStream.newInstance(input, BUFFER_SIZE);
    }

    /**
     * Read one length-delimited message from a {@link CodedInputStream} created by
     * {@link #delimitedInput}. Returns null at end of stream.
     * <p>
     * The wire format is the same as {@code writeDelimitedTo}/{@code parseDelimitedFrom}:
     * a varint length followed by the message bytes.
     */
    static <X extends MessageLite> X readDelimited(CodedInputStream input, Parser<X> parser) throws IOException {
        if ( input.isAtEnd() )
            return null;
        int size = input.readRawVarint32();
        int oldLimit = input.pushLimit(size);
        X message = parser.parseFrom(input);
        input.checkLastTagWas(0);
        input.popLimit(oldLimit);
        // Stop the bytes-read counter growing without bound on a long stream.
        input.resetSizeCounter();
        return message;
    }

    /**
     * Create a {@link CodedOutputStream} for writing a length-delimited stream of
     * messages with {@link #writeDelimited}. The caller must call
     * {@link CodedOutputStream#flush()} when the stream is complete.
     * <p>
     * As for reading, one {@code CodedOutputStream} serves the whole stream.
     * {@code MessageLite.writeDelimitedTo(OutputStream)} creates a new
     * {@code CodedOutputStream}, with its own buffer, for every message.
     */
    static CodedOutputStream delimitedOutput(OutputStream output) {
        return CodedOutputStream.newInstance(output, BUFFER_SIZE);
    }

    /**
     * Write one length-delimited message to a {@link CodedOutputStream} created by
     * {@link #delimitedOutput}.
     */
    static void writeDelimited(MessageLite message, CodedOutputStream output) {
        try {
            output.writeUInt32NoTag(message.getSerializedSize());
            message.writeTo(output);
        } catch (IOException e) { IO.exception(e); }
    }

    static void flush(CodedOutputStream output) {
        try {
            output.flush();
        } catch (IOException e) { IO.exception(e); }
    }

    public static RDF_Triple rdfTriple(Triple triple, RDF_Triple.Builder tripleBuilder, RDF_Term.Builder termBuilder) {
        tripleBuilder.clear();
        tripleBuilder.setS(rdfTerm(triple.getSubject(), termBuilder));
        tripleBuilder.setP(rdfTerm(triple.getPredicate(), termBuilder));
        tripleBuilder.setO(rdfTerm(triple.getObject(), termBuilder));
        return tripleBuilder.build();
    }

    public static RDF_Quad rdfQuad(Quad quad, RDF_Quad.Builder quadBuilder, RDF_Term.Builder termBuilder) {
        quadBuilder.clear();
        if ( quad.getGraph() != null )
            quadBuilder.setG(rdfTerm(quad.getGraph(), termBuilder));
        quadBuilder.setS(rdfTerm(quad.getSubject(), termBuilder));
        quadBuilder.setP(rdfTerm(quad.getPredicate(), termBuilder));
        quadBuilder.setO(rdfTerm(quad.getObject(), termBuilder));
        return quadBuilder.build();
    }

    public static RDF_Term rdfTerm(Node node, RDF_Term.Builder termBuilder) {
        return rdfTerm(node, termBuilder, false);
    }

    static RDF_Term rdfTerm(Node node, RDF_Term.Builder termBuilder, boolean encodeValues) {
        termBuilder.clear();
        RDF_Term term = ProtobufConvert.toProtobuf(node, PMAP0, termBuilder, encodeValues);
        return term;
    }


    /** Visitor dispatch for {@link RDF_StreamRow} */
    static boolean visit(RDF_StreamRow x, VisitorStreamRowProtoRDF visitor) {
        if ( x == null )
            return false;
        switch(x.getRowCase()) {
            case BASE :
                visitor.visit(x.getBase());
                return true;
            case PREFIXDECL :
                visitor.visit(x.getPrefixDecl());
                return true;
            case QUAD :
                visitor.visit(x.getQuad());
                return true;
            case TRIPLE :
                visitor.visit(x.getTriple());
                return true;
            case ROW_NOT_SET :
                throw new InternalErrorException();
        }
        return false;
    }
}
