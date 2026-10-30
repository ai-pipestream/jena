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

package org.apache.jena.rdfpatch.binary;

import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Consumer;

import org.apache.jena.atlas.io.IO;
import org.apache.jena.graph.Node;
import org.apache.jena.rdfpatch.PatchException;
import org.apache.jena.rdfpatch.RDFChanges;
import org.apache.jena.rdfpatch.RDFPatch;
import org.apache.jena.rdfpatch.text.RDFChangesWriterText;
import org.apache.jena.riot.protobuf.ProtobufConvert;
import org.apache.jena.riot.protobuf.RiotProtobufException;
import org.apache.jena.riot.protobuf.wire.PB_RDF.PatchTxn;
import org.apache.jena.riot.protobuf.wire.PB_RDF.Patch_Data_Add;
import org.apache.jena.riot.protobuf.wire.PB_RDF.Patch_Data_Del;
import org.apache.jena.riot.protobuf.wire.PB_RDF.Patch_Header;
import org.apache.jena.riot.protobuf.wire.PB_RDF.Patch_Prefix_Add;
import org.apache.jena.riot.protobuf.wire.PB_RDF.Patch_Prefix_Del;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Patch;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Patch_Row;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Term;

/**
 * Write an {@link RDFPatch} or {@link RDFChanges} stream in Protobuf binary format.
 * <p>
 * Thread safety is implementation specific; instances of this class are not thread safe.
 *
 * @see RDFChangesWriterBinary
 * @see RDFChangesWriterText
 */
public class RDFChangesWriterProtobuf implements RDFChanges, AutoCloseable {

    private static final Runnable NO_OP = () -> {};
    private static final String ERR_PATCH_NULL = "patch must not be null";
    private static final String ERR_FILENAME_NULL = "filename must not be null";
    private static final String ERR_OUT_NULL = "out must not be null";
    private static final String ERR_ACTION_NULL = "action must not be null";
    private static final String ERR_ROW_CONSUMER_NULL = "rowConsumer must not be null";
    private static final String ERR_ON_FINISH_NULL = "onFinish must not be null";
    private static final String ERR_FIELD_NULL = "field must not be null";
    private static final String ERR_VALUE_NULL = "value must not be null";
    private static final String ERR_PREFIX_NULL = "prefix must not be null";
    private static final String ERR_URI_STR_NULL = "uriStr must not be null";
    private static final String ERR_PROTOBUF_WRITE = "Protobuf write exception";

    // Workspace - reused builders
    private final RDF_Term.Builder termBuilder = RDF_Term.newBuilder();
    private final Patch_Header.Builder headerBuilder = Patch_Header.newBuilder();
    private final Patch_Data_Add.Builder dataAddBuilder = Patch_Data_Add.newBuilder();
    private final Patch_Data_Del.Builder dataDelBuilder = Patch_Data_Del.newBuilder();
    private final Patch_Prefix_Add.Builder prefixAddBuilder = Patch_Prefix_Add.newBuilder();
    private final Patch_Prefix_Del.Builder prefixDelBuilder = Patch_Prefix_Del.newBuilder();
    private final RDF_Patch_Row.Builder rowBuilder = RDF_Patch_Row.newBuilder();

    private final Consumer<RDF_Patch_Row> rowConsumer;
    private final Runnable onFinish;

    /**
     * Write an {@link RDFPatch} as length-delimited {@link RDF_Patch_Row} messages to a file.
     *
     * @param patch the patch to write
     * @param filename destination file path
     * @throws IllegalArgumentException Thrown if {@code patch} or {@code filename} is {@code null}
     */
    public static void write(RDFPatch patch, String filename) {
        if ( patch == null )
            throw new IllegalArgumentException(ERR_PATCH_NULL);
        if ( filename == null )
            throw new IllegalArgumentException(ERR_FILENAME_NULL);
        try ( OutputStream out = IO.openOutputFile(filename) ) {
            write(patch, out);
        } catch (IOException ex) { IO.exception(ex); }
    }

    /**
     * Write an {@link RDFPatch} as length-delimited {@link RDF_Patch_Row} messages to an {@link OutputStream}.
     *
     * @param patch the patch to write
     * @param out destination output stream
     * @throws IllegalArgumentException Thrown if {@code patch} or {@code out} is {@code null}
     */
    public static void write(RDFPatch patch, OutputStream out) {
        if ( patch == null )
            throw new IllegalArgumentException(ERR_PATCH_NULL);
        if ( out == null )
            throw new IllegalArgumentException(ERR_OUT_NULL);
        writeProtobuf(out, patch::apply);
    }

    /**
     * Invoke {@code action} with an {@link RDFChanges} writer that emits length-delimited
     * {@link RDF_Patch_Row} messages to {@code out}, bracketed by {@link #start()} and {@link #finish()}.
     *
     * @param out destination output stream
     * @param action callback that emits patch events
     * @throws IllegalArgumentException Thrown if {@code out} or {@code action} is {@code null}
     */
    public static void writeProtobuf(OutputStream out, Consumer<RDFChanges> action) {
        if ( out == null )
            throw new IllegalArgumentException(ERR_OUT_NULL);
        if ( action == null )
            throw new IllegalArgumentException(ERR_ACTION_NULL);
        RDFChanges writer = create(out);
        writer.start();
        try {
            action.accept(writer);
        } finally {
            writer.finish();
        }
    }

    /**
     * Return an {@link RDFChanges} that writes length-delimited {@link RDF_Patch_Row} messages to {@code out}.
     * Callers must invoke {@link RDFChanges#start()} and {@link RDFChanges#finish()}.
     *
     * @param out destination output stream
     * @return an {@link RDFChanges} writer
     * @throws IllegalArgumentException Thrown if {@code out} is {@code null}
     */
    public static RDFChanges create(OutputStream out) {
        if ( out == null )
            throw new IllegalArgumentException(ERR_OUT_NULL);
        OutputStream buffered = IO.ensureBuffered(out);
        Consumer<RDF_Patch_Row> consumer = r -> {
            try {
                r.writeDelimitedTo(buffered);
            } catch (IOException ex) {
                throw new PatchException(ERR_PROTOBUF_WRITE, ex);
            }
        };
        return new RDFChangesWriterProtobuf(consumer, () -> IO.flush(buffered));
    }

    /**
     * Return an {@link RDFChanges} that emits each event as an in-memory {@link RDF_Patch_Row} message
     * to {@code rowConsumer} without intermediate byte-stream serialization.
     *
     * @param rowConsumer consumer receiving each encoded {@link RDF_Patch_Row}
     * @return an {@link RDFChanges} adapter
     * @throws IllegalArgumentException Thrown if {@code rowConsumer} is {@code null}
     */
    public static RDFChanges create(Consumer<RDF_Patch_Row> rowConsumer) {
        return create(rowConsumer, NO_OP);
    }

    /**
     * Return an {@link RDFChanges} that emits each event as an in-memory {@link RDF_Patch_Row} message
     * to {@code rowConsumer} and invokes {@code onFinish} when {@link RDFChanges#finish()} is called.
     *
     * @param rowConsumer consumer receiving each encoded {@link RDF_Patch_Row}
     * @param onFinish callback invoked when {@link RDFChanges#finish()} is called
     * @return an {@link RDFChanges} adapter
     * @throws IllegalArgumentException Thrown if {@code rowConsumer} or {@code onFinish} is {@code null}
     */
    public static RDFChanges create(Consumer<RDF_Patch_Row> rowConsumer, Runnable onFinish) {
        if ( rowConsumer == null )
            throw new IllegalArgumentException(ERR_ROW_CONSUMER_NULL);
        if ( onFinish == null )
            throw new IllegalArgumentException(ERR_ON_FINISH_NULL);
        return new RDFChangesWriterProtobuf(rowConsumer, onFinish);
    }

    /**
     * Encode an entire {@link RDFPatch} into a single {@link RDF_Patch} message containing all patch rows.
     *
     * @param patch the patch to encode
     * @return the encoded {@link RDF_Patch} message
     * @throws IllegalArgumentException Thrown if {@code patch} is {@code null}
     */
    public static RDF_Patch toProtobuf(RDFPatch patch) {
        if ( patch == null )
            throw new IllegalArgumentException(ERR_PATCH_NULL);
        RDF_Patch.Builder builder = RDF_Patch.newBuilder();
        RDFChanges writer = new RDFChangesWriterProtobuf(builder::addRow, NO_OP);
        writer.start();
        try {
            patch.apply(writer);
        } finally {
            writer.finish();
        }
        return builder.build();
    }

    /**
     * Construct a writer forwarding encoded {@link RDF_Patch_Row} messages to {@code rowConsumer}.
     *
     * @param rowConsumer consumer receiving each encoded {@link RDF_Patch_Row}
     * @param onFinish callback invoked when {@link #finish()} is called
     * @throws IllegalArgumentException Thrown if {@code rowConsumer} or {@code onFinish} is {@code null}
     */
    public RDFChangesWriterProtobuf(Consumer<RDF_Patch_Row> rowConsumer, Runnable onFinish) {
        if ( rowConsumer == null )
            throw new IllegalArgumentException(ERR_ROW_CONSUMER_NULL);
        if ( onFinish == null )
            throw new IllegalArgumentException(ERR_ON_FINISH_NULL);
        this.rowConsumer = rowConsumer;
        this.onFinish = onFinish;
    }

    private void emit() {
        rowConsumer.accept(rowBuilder.build());
        rowBuilder.clear();
    }

    private RDF_Term encodeNode(Node node) {
        if ( node == null )
            throw new PatchException("Node must not be null");
        termBuilder.clear();
        try {
            return ProtobufConvert.toProtobuf(node, termBuilder, false);
        } catch (RiotProtobufException ex) {
            throw new PatchException("Node conversion not supported: " + node, ex);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void start() {}

    /** {@inheritDoc} */
    @Override
    public void finish() {
        onFinish.run();
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        finish();
    }

    /** {@inheritDoc} */
    @Override
    public void header(String field, Node value) {
        if ( field == null )
            throw new IllegalArgumentException(ERR_FIELD_NULL);
        if ( value == null )
            throw new IllegalArgumentException(ERR_VALUE_NULL);
        rowBuilder.clear();
        headerBuilder.clear();
        headerBuilder.setName(field);
        headerBuilder.setValue(encodeNode(value));
        rowBuilder.setHeader(headerBuilder.build());
        emit();
    }

    /** {@inheritDoc} */
    @Override
    public void add(Node g, Node s, Node p, Node o) {
        rowBuilder.clear();
        dataAddBuilder.clear();
        dataAddBuilder.setS(encodeNode(s));
        dataAddBuilder.setP(encodeNode(p));
        dataAddBuilder.setO(encodeNode(o));
        if ( g != null )
            dataAddBuilder.setG(encodeNode(g));
        rowBuilder.setDataAdd(dataAddBuilder.build());
        emit();
    }

    /** {@inheritDoc} */
    @Override
    public void delete(Node g, Node s, Node p, Node o) {
        rowBuilder.clear();
        dataDelBuilder.clear();
        dataDelBuilder.setS(encodeNode(s));
        dataDelBuilder.setP(encodeNode(p));
        dataDelBuilder.setO(encodeNode(o));
        if ( g != null )
            dataDelBuilder.setG(encodeNode(g));
        rowBuilder.setDataDel(dataDelBuilder.build());
        emit();
    }

    /** {@inheritDoc} */
    @Override
    public void addPrefix(Node gn, String prefix, String uriStr) {
        if ( prefix == null )
            throw new IllegalArgumentException(ERR_PREFIX_NULL);
        if ( uriStr == null )
            throw new IllegalArgumentException(ERR_URI_STR_NULL);
        rowBuilder.clear();
        prefixAddBuilder.clear();
        if ( gn != null )
            prefixAddBuilder.setGraphNode(encodeNode(gn));
        prefixAddBuilder.setPrefix(prefix);
        prefixAddBuilder.setIriStr(uriStr);
        rowBuilder.setPrefixAdd(prefixAddBuilder.build());
        emit();
    }

    /** {@inheritDoc} */
    @Override
    public void deletePrefix(Node gn, String prefix) {
        if ( prefix == null )
            throw new IllegalArgumentException(ERR_PREFIX_NULL);
        rowBuilder.clear();
        prefixDelBuilder.clear();
        if ( gn != null )
            prefixDelBuilder.setGraphNode(encodeNode(gn));
        prefixDelBuilder.setPrefix(prefix);
        rowBuilder.setPrefixDel(prefixDelBuilder.build());
        emit();
    }

    /** {@inheritDoc} */
    @Override
    public void txnBegin() {
        rowBuilder.clear();
        rowBuilder.setTxn(PatchTxn.TX);
        emit();
    }

    /** {@inheritDoc} */
    @Override
    public void txnCommit() {
        rowBuilder.clear();
        rowBuilder.setTxn(PatchTxn.TC);
        emit();
    }

    /** {@inheritDoc} */
    @Override
    public void txnAbort() {
        rowBuilder.clear();
        rowBuilder.setTxn(PatchTxn.TA);
        emit();
    }

    /** {@inheritDoc} */
    @Override
    public void segment() {
        rowBuilder.clear();
        rowBuilder.setTxn(PatchTxn.Segment);
        emit();
    }
}
