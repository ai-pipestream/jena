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
import java.util.function.Consumer;

import com.google.protobuf.CodedOutputStream;

import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.protobuf.wire.PB_RDF.*;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapFactory;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;

/** Encode StreamRDF in RDF_StreamRow and send to a handler.
 *
 * @see Protobuf2StreamRDF for the reverse process.
 */
public class StreamRDF2Protobuf implements StreamRDF, AutoCloseable
{
    private static final Runnable NO_OP = () -> {};
    private static final String ERR_OUTPUT_STREAM_NULL = "outputStream must not be null";
    private static final String ERR_ROW_HANDLER_NULL = "rowHandler must not be null";
    private static final String ERR_BATCH_HANDLER_NULL = "batchHandler must not be null";
    private static final String ERR_AT_END_NULL = "atEnd must not be null";
    private static final String ERR_STREAM_NULL = "stream must not be null";

    // No REPEAT support.

   // OutputStream + delimited.
    private PrefixMap pmap = PrefixMapFactory.create();
    private final boolean encodeValues;
    private final Consumer<RDF_StreamRow> rowHandler;
    private final Runnable onFlush;
    private final Runnable andFinally;

    /**
     * Create a {@link StreamRDF} that writes length-delimited {@link RDF_StreamRow} messages to an {@link OutputStream}.
     *
     * @param outputStream the destination output stream
     * @param withValues {@code true} to encode numeric literals by value
     * @return a {@link StreamRDF} adapter
     * @throws IllegalArgumentException Thrown if {@code outputStream} is {@code null}
     */
    public static StreamRDF createDelimited(OutputStream outputStream, boolean withValues) {
        if ( outputStream == null )
            throw new IllegalArgumentException(ERR_OUTPUT_STREAM_NULL);
        CodedOutputStream codedOutput = PBufRDF.delimitedOutput(outputStream);
        Consumer<RDF_StreamRow> output = sr->PBufRDF.writeDelimited(sr, codedOutput);
        Runnable flushAction = () -> {
            PBufRDF.flush(codedOutput);
            IO.flush(outputStream);
        };
        return new StreamRDF2Protobuf(output, withValues, flushAction, flushAction);
    }

    /**
     * Create a {@link StreamRDF} that emits each event as an {@link RDF_StreamRow} to the given consumer.
     *
     * @param rowHandler consumer receiving each encoded {@link RDF_StreamRow}
     * @return a {@link StreamRDF} adapter
     * @throws IllegalArgumentException Thrown if {@code rowHandler} is {@code null}
     */
    public static StreamRDF create(Consumer<RDF_StreamRow> rowHandler) {
        return create(rowHandler, false, NO_OP);
    }

    /**
     * Create a {@link StreamRDF} that emits each event as an {@link RDF_StreamRow} to the given consumer.
     *
     * @param rowHandler consumer receiving each encoded {@link RDF_StreamRow}
     * @param withValues {@code true} to encode numeric literals by value
     * @return a {@link StreamRDF} adapter
     * @throws IllegalArgumentException Thrown if {@code rowHandler} is {@code null}
     */
    public static StreamRDF create(Consumer<RDF_StreamRow> rowHandler, boolean withValues) {
        return create(rowHandler, withValues, NO_OP);
    }

    /**
     * Create a {@link StreamRDF} that emits each event as an {@link RDF_StreamRow} to the given consumer
     * and runs a completion callback on {@link #finish()}.
     * <p>
     * Because prefix declarations update a stateful {@link PrefixMap} used to abbreviate subsequent IRIs,
     * emitted {@link RDF_StreamRow} messages must be delivered to the consumer in stream order.
     *
     * @param rowHandler consumer receiving each encoded {@link RDF_StreamRow}
     * @param withValues {@code true} to encode numeric literals by value
     * @param atEnd callback invoked when {@link #finish()} is called
     * @return a {@link StreamRDF} adapter
     * @throws IllegalArgumentException Thrown if {@code rowHandler} or {@code atEnd} is {@code null}
     */
    public static StreamRDF create(Consumer<RDF_StreamRow> rowHandler, boolean withValues, Runnable atEnd) {
        if ( rowHandler == null )
            throw new IllegalArgumentException(ERR_ROW_HANDLER_NULL);
        if ( atEnd == null )
            throw new IllegalArgumentException(ERR_AT_END_NULL);
        return new StreamRDF2Protobuf(rowHandler, withValues, NO_OP, atEnd);
    }

    /**
     * Create a {@link StreamRDF} that batches encoded {@link RDF_StreamRow} items into {@link RDF_Stream}
     * messages of at most {@code batchSize} rows.
     * <p>
     * Because prefix declarations update a stateful {@link PrefixMap} used to abbreviate subsequent IRIs,
     * emitted {@link RDF_Stream} batches must be delivered to the consumer in stream order.
     *
     * @param batchHandler consumer receiving each non-empty {@link RDF_Stream} batch
     * @param batchSize maximum number of rows per emitted {@link RDF_Stream} message (must be positive)
     * @param withValues {@code true} to encode numeric literals by value
     * @return a batched {@link StreamRDF} adapter
     * @throws IllegalArgumentException Thrown if {@code batchHandler} is {@code null} or {@code batchSize <= 0}
     */
    public static StreamRDF createBatched(Consumer<RDF_Stream> batchHandler, int batchSize, boolean withValues) {
        return createBatched(batchHandler, batchSize, withValues, NO_OP);
    }

    /**
     * Create a {@link StreamRDF} that batches encoded {@link RDF_StreamRow} items into {@link RDF_Stream}
     * messages of at most {@code batchSize} rows and runs a completion callback on {@link #finish()}.
     * <p>
     * Because prefix declarations update a stateful {@link PrefixMap} used to abbreviate subsequent IRIs,
     * emitted {@link RDF_Stream} batches must be delivered to the consumer in stream order.
     *
     * @param batchHandler consumer receiving each non-empty {@link RDF_Stream} batch
     * @param batchSize maximum number of rows per emitted {@link RDF_Stream} message (must be positive)
     * @param withValues {@code true} to encode numeric literals by value
     * @param atEnd callback invoked when {@link #finish()} is called after flushing any pending batch
     * @return a batched {@link StreamRDF} adapter
     * @throws IllegalArgumentException Thrown if {@code batchHandler} or {@code atEnd} is {@code null}, or {@code batchSize <= 0}
     */
    public static StreamRDF createBatched(Consumer<RDF_Stream> batchHandler, int batchSize, boolean withValues, Runnable atEnd) {
        if ( batchHandler == null )
            throw new IllegalArgumentException(ERR_BATCH_HANDLER_NULL);
        if ( batchSize <= 0 )
            throw new IllegalArgumentException("batchSize must be positive: " + batchSize);
        if ( atEnd == null )
            throw new IllegalArgumentException(ERR_AT_END_NULL);
        RDF_Stream.Builder batchBuilder = RDF_Stream.newBuilder();
        Runnable flushBatch = () -> {
            if ( batchBuilder.getRowCount() > 0 ) {
                batchHandler.accept(batchBuilder.build());
                batchBuilder.clear();
            }
        };
        Consumer<RDF_StreamRow> rowAccum = row -> {
            batchBuilder.addRow(row);
            if ( batchBuilder.getRowCount() >= batchSize ) {
                batchHandler.accept(batchBuilder.build());
                batchBuilder.clear();
            }
        };
        return new StreamRDF2Protobuf(rowAccum, withValues, flushBatch, () -> {
            flushBatch.run();
            atEnd.run();
        });
    }

    /**
     * Write a {@link StreamRDF} sequence as a single non-delimited {@link RDF_Stream} block to an {@link OutputStream}.
     *
     * @param outputStream the destination output stream
     * @param stream callback that feeds items into the {@link StreamRDF}
     * @param withValues {@code true} to encode numeric literals by value
     * @throws IllegalArgumentException Thrown if {@code outputStream} or {@code stream} is {@code null}
     */
    public static void writeBlk(OutputStream outputStream, Consumer<StreamRDF> stream, boolean withValues) {
        if ( outputStream == null )
            throw new IllegalArgumentException(ERR_OUTPUT_STREAM_NULL);
        if ( stream == null )
            throw new IllegalArgumentException(ERR_STREAM_NULL);
        RDF_Stream.Builder builder = RDF_Stream.newBuilder();
        Consumer<RDF_StreamRow> output = sr->builder.addRow(sr);
        Runnable flushAction = ()->IO.flush(outputStream);
        StreamRDF2Protobuf processor = new StreamRDF2Protobuf(output, withValues, flushAction, flushAction);
        stream.accept(processor);
        RDF_Stream pbStream = builder.build();
        try {
            pbStream.writeTo(outputStream);
        } catch (IOException ex) { IO.exception(ex); }
    }


    private StreamRDF2Protobuf(Consumer<RDF_StreamRow> rowHandler, boolean encodeValues, Runnable onFlush, Runnable atEnd) {
        this.pmap = PrefixMapFactory.create();
        this.encodeValues = encodeValues;
        this.rowHandler = rowHandler;
        this.onFlush = onFlush;
        this.andFinally = atEnd;
    }

    private RDF_StreamRow.Builder streamRowBuilder = RDF_StreamRow.newBuilder();
    private RDF_Triple.Builder tripleBuilder = RDF_Triple.newBuilder();
    private RDF_Quad.Builder quadBuilder = RDF_Quad.newBuilder();
    private RDF_PrefixDecl.Builder prefixBuilder = RDF_PrefixDecl.newBuilder();
    private RDF_IRI.Builder baseBuilder = RDF_IRI.newBuilder();
    private RDF_Term.Builder termBuilder = RDF_Term.newBuilder();

    /** {@inheritDoc} */
    @Override
    public void start() {}

    /** {@inheritDoc} */
    @Override
    public void flush() {
        onFlush.run();
    }

    /** {@inheritDoc} */
    @Override
    public void finish() {
        andFinally.run();
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        finish();
    }


    /** {@inheritDoc} */
    @Override
    public void base(String base) {
        streamRowBuilder.clear();
        baseBuilder.clear();
        baseBuilder.setIri(base);
        streamRowBuilder.setBase(baseBuilder.build());
        rowHandler.accept(streamRowBuilder.build());
    }

    /** {@inheritDoc} */
    @Override
    public void prefix(String prefix, String iri) {
        try {
            pmap.add(prefix, iri);
        } catch (RiotException ex) {
            Log.warn(this, "Prefix mapping error", ex);
        }
        streamRowBuilder.clear();
        prefixBuilder.clear();
        prefixBuilder.setPrefix(prefix);
        prefixBuilder.setUri(iri);
        streamRowBuilder.setPrefixDecl(prefixBuilder.build());
        rowHandler.accept(streamRowBuilder.build());
    }

    /** {@inheritDoc} */
    @Override
    public void version(String version) {}

    /** {@inheritDoc} */
    @Override
    public void triple(Triple triple) {
        streamRowBuilder.clear();
        RDF_Triple triplePB = PBufRDF.rdfTriple(triple, pmap, tripleBuilder, termBuilder, encodeValues);
        streamRowBuilder.setTriple(triplePB);
        rowHandler.accept(streamRowBuilder.build());
    }

    /** {@inheritDoc} */
    @Override
    public void quad(Quad quad) {
        streamRowBuilder.clear();
        RDF_Quad quadPB = PBufRDF.rdfQuad(quad, pmap, quadBuilder, termBuilder, encodeValues);
        streamRowBuilder.setQuad(quadPB);
        rowHandler.accept(streamRowBuilder.build());
    }
}
