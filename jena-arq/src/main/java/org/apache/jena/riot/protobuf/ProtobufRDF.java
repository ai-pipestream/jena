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
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.lib.Cache;
import org.apache.jena.graph.Node;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_DataTuple;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Stream;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_StreamRow;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_VarTuple;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapFactory;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.thrift.ThriftRDF;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.exec.RowSet;
import org.apache.jena.sparql.exec.RowSetStream;

/**
 * Operations on binary RDF using <a href="https://developers.google.com/protocol-buffers">Google Protobuf</a>.
 * <p>
 * An encoding use Apache Thrift is available in {@link ThriftRDF}.
 */
public class ProtobufRDF {

    /** Default maximum number of {@link RDF_StreamRow} items per batched {@link RDF_Stream} message. */
    public static final int DEFAULT_BATCH_SIZE = 1000;

    /**
     * Create an {@link StreamRDF} for output.  A filename ending {@code .gz} will have
     * a gzip compressor added to the output path. A filename of "-" is {@code System.out}.
     * The file is closed when {@link StreamRDF#finish()} is called unless it is {@code System.out}.
     * Call {@link StreamRDF#start()}...{@link StreamRDF#finish()}.
     *
     * @param filename The file
     * @return StreamRDF A stream to send to.
     */
    public static StreamRDF streamToFile(String filename) {
        return streamToFile(filename, false);
    }

    /**
     * Create an {@link StreamRDF} for output.  A filenames ending {@code .gz} or {@code .bz2} will have
     * the respective compressor added to the output path. A filename of "-" is {@code System.out}.
     * The file is closed when {@link StreamRDF#finish()} is called unless it is {@code System.out}.
     * Call {@link StreamRDF#start()}...{@link StreamRDF#finish()}.
     *
     * @param filename The file
     * @param withValues - whether to encode numeric values as values.
     * @return StreamRDF A stream to send to.
     */
    public static StreamRDF streamToFile(String filename, boolean withValues) {
        OutputStream out = IO.openOutputFile(filename);
        out = IO.ensureBuffered(out);
        return streamToOutputStream(out, withValues);
    }

    /**
     * An {@link StreamRDF} that writes to an output stream.
     * The output stream is written with "writeDelimitedTo".
     *
     * @param output the destination output stream
     * @return a {@link StreamRDF} writing length-delimited rows
     */
    public static StreamRDF streamToOutputStream(OutputStream output) {
        return streamToOutputStream(output, false);
    }

    /**
     * An {@link StreamRDF} that writes to an output stream.
     * The output stream is written with "writeDelimitedTo".
     *
     * @param output the destination output stream
     * @param withValues {@code true} to encode numeric literals by value
     * @return a {@link StreamRDF} writing length-delimited rows
     */
    public static StreamRDF streamToOutputStream(OutputStream output, boolean withValues) {
        output = IO.ensureBuffered(output);
        try {
            return StreamRDF2Protobuf.createDelimited(output, withValues);
        } finally { IO.flush(output); }
    }

    /**
     * Create a {@link StreamRDF} that emits each event as an in-memory {@link RDF_StreamRow} message
     * to {@code rowConsumer} without intermediate byte-stream serialization.
     *
     * @param rowConsumer consumer receiving each encoded {@link RDF_StreamRow}
     * @return a {@link StreamRDF} adapter
     * @throws IllegalArgumentException Thrown if {@code rowConsumer} is {@code null}
     */
    public static StreamRDF streamToRowConsumer(Consumer<RDF_StreamRow> rowConsumer) {
        return StreamRDF2Protobuf.create(rowConsumer);
    }

    /**
     * Create a {@link StreamRDF} that emits each event as an in-memory {@link RDF_StreamRow} message
     * to {@code rowConsumer} without intermediate byte-stream serialization.
     *
     * @param rowConsumer consumer receiving each encoded {@link RDF_StreamRow}
     * @param withValues {@code true} to encode numeric literals by value
     * @return a {@link StreamRDF} adapter
     * @throws IllegalArgumentException Thrown if {@code rowConsumer} is {@code null}
     */
    public static StreamRDF streamToRowConsumer(Consumer<RDF_StreamRow> rowConsumer, boolean withValues) {
        return StreamRDF2Protobuf.create(rowConsumer, withValues);
    }

    /**
     * Create a {@link StreamRDF} that emits each event as an in-memory {@link RDF_StreamRow} message
     * to {@code rowConsumer} and invokes {@code onFinish} when {@link StreamRDF#finish()} is called.
     *
     * @param rowConsumer consumer receiving each encoded {@link RDF_StreamRow}
     * @param withValues {@code true} to encode numeric literals by value
     * @param onFinish callback invoked when {@link StreamRDF#finish()} is called
     * @return a {@link StreamRDF} adapter
     * @throws IllegalArgumentException Thrown if {@code rowConsumer} or {@code onFinish} is {@code null}
     */
    public static StreamRDF streamToRowConsumer(Consumer<RDF_StreamRow> rowConsumer, boolean withValues, Runnable onFinish) {
        return StreamRDF2Protobuf.create(rowConsumer, withValues, onFinish);
    }

    /**
     * Create a {@link StreamRDF} that batches encoded {@link RDF_StreamRow} items into {@link RDF_Stream}
     * messages of at most {@link #DEFAULT_BATCH_SIZE} rows.
     *
     * @param batchConsumer consumer receiving each non-empty {@link RDF_Stream} batch
     * @return a batched {@link StreamRDF} adapter
     * @throws IllegalArgumentException Thrown if {@code batchConsumer} is {@code null}
     */
    public static StreamRDF streamToBatchConsumer(Consumer<RDF_Stream> batchConsumer) {
        return streamToBatchConsumer(batchConsumer, DEFAULT_BATCH_SIZE, false);
    }

    /**
     * Create a {@link StreamRDF} that batches encoded {@link RDF_StreamRow} items into {@link RDF_Stream}
     * messages of at most {@code batchSize} rows.
     *
     * @param batchConsumer consumer receiving each non-empty {@link RDF_Stream} batch
     * @param batchSize maximum number of rows per emitted {@link RDF_Stream} message (must be positive)
     * @return a batched {@link StreamRDF} adapter
     * @throws IllegalArgumentException Thrown if {@code batchConsumer} is {@code null} or {@code batchSize <= 0}
     */
    public static StreamRDF streamToBatchConsumer(Consumer<RDF_Stream> batchConsumer, int batchSize) {
        return streamToBatchConsumer(batchConsumer, batchSize, false);
    }

    /**
     * Create a {@link StreamRDF} that batches encoded {@link RDF_StreamRow} items into {@link RDF_Stream}
     * messages of at most {@code batchSize} rows.
     *
     * @param batchConsumer consumer receiving each non-empty {@link RDF_Stream} batch
     * @param batchSize maximum number of rows per emitted {@link RDF_Stream} message (must be positive)
     * @param withValues {@code true} to encode numeric literals by value
     * @return a batched {@link StreamRDF} adapter
     * @throws IllegalArgumentException Thrown if {@code batchConsumer} is {@code null} or {@code batchSize <= 0}
     */
    public static StreamRDF streamToBatchConsumer(Consumer<RDF_Stream> batchConsumer, int batchSize, boolean withValues) {
        return StreamRDF2Protobuf.createBatched(batchConsumer, batchSize, withValues);
    }

    /**
     * Create a {@link StreamRDF} that batches encoded {@link RDF_StreamRow} items into {@link RDF_Stream}
     * messages of at most {@code batchSize} rows and invokes {@code onFinish} on {@link StreamRDF#finish()}.
     *
     * @param batchConsumer consumer receiving each non-empty {@link RDF_Stream} batch
     * @param batchSize maximum number of rows per emitted {@link RDF_Stream} message (must be positive)
     * @param withValues {@code true} to encode numeric literals by value
     * @param onFinish callback invoked when {@link StreamRDF#finish()} is called after flushing any pending batch
     * @return a batched {@link StreamRDF} adapter
     * @throws IllegalArgumentException Thrown if {@code batchConsumer} or {@code onFinish} is {@code null}, or {@code batchSize <= 0}
     */
    public static StreamRDF streamToBatchConsumer(Consumer<RDF_Stream> batchConsumer, int batchSize, boolean withValues, Runnable onFinish) {
        return StreamRDF2Protobuf.createBatched(batchConsumer, batchSize, withValues, onFinish);
    }

    /**
     * Return a {@link Consumer} of {@link RDF_StreamRow} messages that decodes and forwards each row
     * to {@code dest} using a fresh {@link PrefixMap} and default URI cache.
     *
     * @param dest the destination {@link StreamRDF}
     * @return a consumer of {@link RDF_StreamRow} messages
     * @throws IllegalArgumentException Thrown if {@code dest} is {@code null}
     */
    public static Consumer<RDF_StreamRow> rowConsumerToStreamRDF(StreamRDF dest) {
        return new Protobuf2StreamRDF(dest);
    }

    /**
     * Return a {@link Consumer} of {@link RDF_StreamRow} messages that decodes and forwards each row
     * to {@code dest} using the supplied {@link PrefixMap} and default URI cache.
     *
     * @param pmap the prefix map to populate and consult
     * @param dest the destination {@link StreamRDF}
     * @return a consumer of {@link RDF_StreamRow} messages
     * @throws IllegalArgumentException Thrown if {@code pmap} or {@code dest} is {@code null}
     */
    public static Consumer<RDF_StreamRow> rowConsumerToStreamRDF(PrefixMap pmap, StreamRDF dest) {
        return new Protobuf2StreamRDF(pmap, dest);
    }

    /**
     * Return a {@link Consumer} of {@link RDF_StreamRow} messages that decodes and forwards each row
     * to {@code dest} using the supplied {@link PrefixMap} and optional URI cache.
     *
     * @param pmap the prefix map to populate and consult
     * @param dest the destination {@link StreamRDF}
     * @param uriCache optional cache for interning URI nodes, or {@code null} to disable caching
     * @return a consumer of {@link RDF_StreamRow} messages
     * @throws IllegalArgumentException Thrown if {@code pmap} or {@code dest} is {@code null}
     */
    public static Consumer<RDF_StreamRow> rowConsumerToStreamRDF(PrefixMap pmap, StreamRDF dest, Cache<String, Node> uriCache) {
        return new Protobuf2StreamRDF(pmap, dest, uriCache);
    }

    /**
     * Return a {@link Consumer} of batched {@link RDF_Stream} messages that decodes and forwards each row
     * to {@code dest} using a fresh {@link PrefixMap} and default URI cache.
     *
     * @param dest the destination {@link StreamRDF}
     * @return a consumer of {@link RDF_Stream} messages
     * @throws IllegalArgumentException Thrown if {@code dest} is {@code null}
     */
    public static Consumer<RDF_Stream> batchConsumerToStreamRDF(StreamRDF dest) {
        Protobuf2StreamRDF decoder = new Protobuf2StreamRDF(dest);
        return decoder::accept;
    }

    /**
     * Return a {@link Consumer} of batched {@link RDF_Stream} messages that decodes and forwards each row
     * to {@code dest} using the supplied {@link PrefixMap} and default URI cache.
     *
     * @param pmap the prefix map to populate and consult
     * @param dest the destination {@link StreamRDF}
     * @return a consumer of {@link RDF_Stream} messages
     * @throws IllegalArgumentException Thrown if {@code pmap} or {@code dest} is {@code null}
     */
    public static Consumer<RDF_Stream> batchConsumerToStreamRDF(PrefixMap pmap, StreamRDF dest) {
        Protobuf2StreamRDF decoder = new Protobuf2StreamRDF(pmap, dest);
        return decoder::accept;
    }

    /**
     * Return a {@link Consumer} of batched {@link RDF_Stream} messages that decodes and forwards each row
     * to {@code dest} using the supplied {@link PrefixMap} and optional URI cache.
     *
     * @param pmap the prefix map to populate and consult
     * @param dest the destination {@link StreamRDF}
     * @param uriCache optional cache for interning URI nodes, or {@code null} to disable caching
     * @return a consumer of {@link RDF_Stream} messages
     * @throws IllegalArgumentException Thrown if {@code pmap} or {@code dest} is {@code null}
     */
    public static Consumer<RDF_Stream> batchConsumerToStreamRDF(PrefixMap pmap, StreamRDF dest, Cache<String, Node> uriCache) {
        Protobuf2StreamRDF decoder = new Protobuf2StreamRDF(pmap, dest, uriCache);
        return decoder::accept;
    }

    /**
     * Decode all rows in a single {@link RDF_Stream} message and send them to {@code dest},
     * bracketed by {@link StreamRDF#start()} and {@link StreamRDF#finish()}.
     *
     * @param stream the {@link RDF_Stream} message to decode
     * @param dest the destination {@link StreamRDF}
     * @throws IllegalArgumentException Thrown if {@code stream} or {@code dest} is {@code null}
     */
    public static void protobufToStreamRDF(RDF_Stream stream, StreamRDF dest) {
        if ( stream == null )
            throw new IllegalArgumentException("stream must not be null");
        if ( dest == null )
            throw new IllegalArgumentException("dest must not be null");
        Protobuf2StreamRDF decoder = new Protobuf2StreamRDF(PrefixMapFactory.create(), dest);
        dest.start();
        try {
            decoder.accept(stream);
        } finally {
            dest.finish();
        }
    }

    /**
     * Decode the contents of the file and send to the {@link StreamRDF}.
     * A filename ending {@code .gz} will have a gzip decompressor added.
     * A filename of "-" is {@code System.in}.
     * @param filename The file.
     * @param dest Sink
     */
    public static void fileToStream(String filename, StreamRDF dest) {
        InputStream in = IO.openFileBuffered(filename);
        inputStreamToStreamRDF(in, dest);
    }

    /**
     * Read an input stream and send item to the {@link StreamRDF}. The input stream
     * will be read with "parseDelimitedTo".
     * @param input InputStream
     * @param stream StreamRDF
     */
    public static void inputStreamToStreamRDF(InputStream input, StreamRDF stream) {
        Protobuf2StreamRDF visitor = new Protobuf2StreamRDF(PrefixMapFactory.create(), stream);
        stream.start();
        try {
            apply(input, visitor);
        }
        finally { stream.finish(); }
    }

    /**
     * Send the contents of a RDF-encoded protobuf file to an "action"
     * @param input   InputStream
     * @param action  Code to act on the row.
     */
    public static void apply(InputStream input, VisitorStreamRowProtoRDF action) {
        input = IO.ensureBuffered(input);
        try {
            while(true) {
                RDF_StreamRow x = RDF_StreamRow.parseDelimitedFrom(input);
                boolean b = PBufRDF.visit(x, action);
                if ( !b )
                    return;
            }
        } catch(IOException ex) { IO.exception(ex); }
    }

    /**
     * Return a streaming {@link RowSet} read from an input stream (with delimiters per row)
     *
     * @param input the source input stream
     * @return a streaming {@link RowSet}
     */
    public static RowSet readRowSet(InputStream input) {
        Protobuf2Binding p2b = new Protobuf2Binding(input);
        return RowSetStream.create(p2b.getVars(), p2b);
    }

    /**
     * Return a streaming {@link RowSet} decoded from an {@link RDF_VarTuple} header message
     * and an iterator of {@link RDF_DataTuple} row messages.
     *
     * @param varTuple the variable header message
     * @param dataTuples iterator supplying {@link RDF_DataTuple} row messages
     * @return a streaming {@link RowSet}
     * @throws IllegalArgumentException Thrown if {@code varTuple} or {@code dataTuples} is {@code null}
     */
    public static RowSet readRowSet(RDF_VarTuple varTuple, Iterator<RDF_DataTuple> dataTuples) {
        Protobuf2Binding p2b = new Protobuf2Binding(varTuple, dataTuples);
        return RowSetStream.create(p2b.getVars(), p2b);
    }

    /**
     * Return a streaming {@link RowSet} decoded from an ordered list of result variables
     * and an iterator of {@link RDF_DataTuple} row messages.
     *
     * @param vars the result variables in column order
     * @param dataTuples iterator supplying {@link RDF_DataTuple} row messages
     * @return a streaming {@link RowSet}
     * @throws IllegalArgumentException Thrown if {@code vars} or {@code dataTuples} is {@code null}
     */
    public static RowSet readRowSet(List<Var> vars, Iterator<RDF_DataTuple> dataTuples) {
        Protobuf2Binding p2b = new Protobuf2Binding(vars, dataTuples);
        return RowSetStream.create(p2b.getVars(), p2b);
    }

    /**
     * Write a {@link RowSet} to an output stream (with delimiters per row)
     *
     * @param out destination output stream
     * @param rowSet the row set to write
     */
    public static void writeRowSet(OutputStream out, RowSet rowSet) {
        writeRowSet(out, rowSet, false);
    }

    /**
     * Write a {@link RowSet} to an output stream (with delimiters per row)
     *
     * @param out destination output stream
     * @param rowSet the row set to write
     * @param withValues {@code true} to encode numeric literals by value
     */
    public static void writeRowSet(OutputStream out, RowSet rowSet, boolean withValues) {
        out = IO.ensureBuffered(out);
        try {
            List<Var> vars = rowSet.getResultVars();
            try ( Binding2Protobuf b2p = new Binding2Protobuf(out, vars, withValues) ) {
                rowSet.forEachRemaining(b2p::output);
            }
        } finally { IO.flush(out); }
    }

    /**
     * Encode a {@link RowSet} as an {@link RDF_VarTuple} header message followed by a stream
     * of {@link RDF_DataTuple} row messages without intermediate byte-stream serialization.
     *
     * @param rowSet the row set to encode
     * @param headerConsumer consumer receiving the initial {@link RDF_VarTuple} header
     * @param rowConsumer consumer receiving each encoded {@link RDF_DataTuple} row
     * @throws IllegalArgumentException Thrown if {@code rowSet}, {@code headerConsumer}, or {@code rowConsumer} is {@code null}
     */
    public static void writeRowSet(RowSet rowSet, Consumer<RDF_VarTuple> headerConsumer, Consumer<RDF_DataTuple> rowConsumer) {
        writeRowSet(rowSet, headerConsumer, rowConsumer, false);
    }

    /**
     * Encode a {@link RowSet} as an {@link RDF_VarTuple} header message followed by a stream
     * of {@link RDF_DataTuple} row messages without intermediate byte-stream serialization.
     *
     * @param rowSet the row set to encode
     * @param headerConsumer consumer receiving the initial {@link RDF_VarTuple} header
     * @param rowConsumer consumer receiving each encoded {@link RDF_DataTuple} row
     * @param withValues {@code true} to encode numeric literals by value
     * @throws IllegalArgumentException Thrown if {@code rowSet}, {@code headerConsumer}, or {@code rowConsumer} is {@code null}
     */
    public static void writeRowSet(RowSet rowSet, Consumer<RDF_VarTuple> headerConsumer, Consumer<RDF_DataTuple> rowConsumer, boolean withValues) {
        if ( rowSet == null )
            throw new IllegalArgumentException("rowSet must not be null");
        List<Var> vars = rowSet.getResultVars();
        try ( Binding2Protobuf b2p = new Binding2Protobuf(headerConsumer, rowConsumer, vars, withValues) ) {
            rowSet.forEachRemaining(b2p::output);
        }
    }

}

