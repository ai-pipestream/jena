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
import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.jena.atlas.io.IO;
import org.apache.jena.graph.Node;
import org.apache.jena.rdfpatch.PatchException;
import org.apache.jena.rdfpatch.PatchHeader;
import org.apache.jena.rdfpatch.PatchProcessor;
import org.apache.jena.rdfpatch.RDFChanges;
import org.apache.jena.rdfpatch.RDFPatch;
import org.apache.jena.rdfpatch.changes.RDFChangesCollector;
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
 * Read a Protobuf binary patch from an {@link InputStream}, {@link Iterator} of {@link RDF_Patch_Row},
 * or {@link RDF_Patch} message.
 *
 * @see PatchProcessor
 * @see RDFPatchReaderBinary
 */
public class RDFPatchReaderProtobuf implements PatchProcessor {

    private static final String ERR_INPUT_NULL = "input must not be null";
    private static final String ERR_ROWS_NULL = "rows must not be null";
    private static final String ERR_PATCH_NULL = "patch must not be null";
    private static final String ERR_CHANGES_NULL = "changes must not be null";
    private static final String ERR_ROW_NULL = "row must not be null";
    private static final String ERR_PROTOBUF_READ = "Protobuf exception";

    private final InputStream input;
    private final Iterator<RDF_Patch_Row> rows;

    private RDFPatchReaderProtobuf(InputStream input, Iterator<RDF_Patch_Row> rows) {
        this.input = input;
        this.rows = rows;
    }

    /** {@inheritDoc} */
    @Override
    public void apply(RDFChanges processor) {
        if ( processor == null )
            throw new IllegalArgumentException(ERR_CHANGES_NULL);
        if ( rows != null ) {
            read(rows, processor);
        } else {
            read(input, processor);
        }
    }

    /**
     * Create a delayed {@link PatchProcessor} reading length-delimited {@link RDF_Patch_Row} messages
     * from {@code input}.
     *
     * @param input source input stream
     * @return a {@link PatchProcessor}
     * @throws IllegalArgumentException Thrown if {@code input} is {@code null}
     */
    public static PatchProcessor create(InputStream input) {
        if ( input == null )
            throw new IllegalArgumentException(ERR_INPUT_NULL);
        return new RDFPatchReaderProtobuf(input, null);
    }

    /**
     * Create a delayed {@link PatchProcessor} reading {@link RDF_Patch_Row} messages from {@code rows}.
     *
     * @param rows iterator of {@link RDF_Patch_Row} messages
     * @return a {@link PatchProcessor}
     * @throws IllegalArgumentException Thrown if {@code rows} is {@code null}
     */
    public static PatchProcessor create(Iterator<RDF_Patch_Row> rows) {
        if ( rows == null )
            throw new IllegalArgumentException(ERR_ROWS_NULL);
        return new RDFPatchReaderProtobuf(null, rows);
    }

    /**
     * Create a {@link PatchProcessor} backed by an in-memory {@link RDF_Patch} message.
     *
     * @param patch the Protobuf patch message
     * @return a {@link PatchProcessor}
     * @throws IllegalArgumentException Thrown if {@code patch} is {@code null}
     */
    public static PatchProcessor create(RDF_Patch patch) {
        if ( patch == null )
            throw new IllegalArgumentException(ERR_PATCH_NULL);
        return processor -> read(patch, processor);
    }

    /**
     * Read length-delimited {@link RDF_Patch_Row} messages from {@code input} into an in-memory {@link RDFPatch}.
     *
     * @param input source input stream
     * @return the collected {@link RDFPatch}
     * @throws IllegalArgumentException Thrown if {@code input} is {@code null}
     */
    public static RDFPatch read(InputStream input) {
        if ( input == null )
            throw new IllegalArgumentException(ERR_INPUT_NULL);
        RDFChangesCollector changes = new RDFChangesCollector();
        read(input, changes);
        return changes.getRDFPatch();
    }

    /**
     * Read {@link RDF_Patch_Row} messages from {@code rows} into an in-memory {@link RDFPatch}.
     *
     * @param rows iterator of {@link RDF_Patch_Row} messages
     * @return the collected {@link RDFPatch}
     * @throws IllegalArgumentException Thrown if {@code rows} is {@code null}
     */
    public static RDFPatch read(Iterator<RDF_Patch_Row> rows) {
        if ( rows == null )
            throw new IllegalArgumentException(ERR_ROWS_NULL);
        RDFChangesCollector changes = new RDFChangesCollector();
        read(rows, changes);
        return changes.getRDFPatch();
    }

    /**
     * Convert an {@link RDF_Patch} block message into an in-memory {@link RDFPatch}.
     *
     * @param patch the Protobuf patch message
     * @return the collected {@link RDFPatch}
     * @throws IllegalArgumentException Thrown if {@code patch} is {@code null}
     */
    public static RDFPatch read(RDF_Patch patch) {
        if ( patch == null )
            throw new IllegalArgumentException(ERR_PATCH_NULL);
        RDFChangesCollector changes = new RDFChangesCollector();
        read(patch, changes);
        return changes.getRDFPatch();
    }

    /**
     * Read the leading {@link PatchHeader} from length-delimited {@link RDF_Patch_Row} messages on {@code input}.
     * <p>
     * This method consumes {@code input} up to and including the first non-header row; the stream is not reset
     * and cannot be passed directly to {@link #read(InputStream)} afterward without reopening or resetting the source.
     *
     * @param input source input stream
     * @return the decoded {@link PatchHeader}
     * @throws IllegalArgumentException Thrown if {@code input} is {@code null}
     */
    public static PatchHeader readHeader(InputStream input) {
        if ( input == null )
            throw new IllegalArgumentException(ERR_INPUT_NULL);
        InputStream buffered = IO.ensureBuffered(input);
        Map<String, Node> header = new LinkedHashMap<>();
        for (;;) {
            RDF_Patch_Row row;
            try {
                row = RDF_Patch_Row.parseDelimitedFrom(buffered);
            } catch (IOException ex) {
                throw new PatchException(ERR_PROTOBUF_READ, ex);
            }
            if ( row == null || !row.hasHeader() )
                break;
            Patch_Header h = row.getHeader();
            header.put(h.getName(), fromProtobuf(h.getValue()));
        }
        return new PatchHeader(header);
    }

    /**
     * Read the leading {@link PatchHeader} from an iterator of {@link RDF_Patch_Row} messages.
     *
     * @param rows iterator of {@link RDF_Patch_Row} messages
     * @return the decoded {@link PatchHeader}
     * @throws IllegalArgumentException Thrown if {@code rows} is {@code null}
     */
    public static PatchHeader readHeader(Iterator<RDF_Patch_Row> rows) {
        if ( rows == null )
            throw new IllegalArgumentException(ERR_ROWS_NULL);
        Map<String, Node> header = new LinkedHashMap<>();
        while ( rows.hasNext() ) {
            RDF_Patch_Row row = rows.next();
            if ( row == null || !row.hasHeader() )
                break;
            Patch_Header h = row.getHeader();
            header.put(h.getName(), fromProtobuf(h.getValue()));
        }
        return new PatchHeader(header);
    }

    /**
     * Read length-delimited {@link RDF_Patch_Row} messages from {@code input} and apply them to {@code changes}.
     *
     * @param input source input stream
     * @param changes destination {@link RDFChanges}
     * @throws IllegalArgumentException Thrown if {@code input} or {@code changes} is {@code null}
     */
    public static void read(InputStream input, RDFChanges changes) {
        if ( input == null )
            throw new IllegalArgumentException(ERR_INPUT_NULL);
        if ( changes == null )
            throw new IllegalArgumentException(ERR_CHANGES_NULL);
        InputStream buffered = IO.ensureBuffered(input);
        changes.start();
        try {
            for (;;) {
                RDF_Patch_Row row;
                try {
                    row = RDF_Patch_Row.parseDelimitedFrom(buffered);
                } catch (IOException ex) {
                    throw new PatchException(ERR_PROTOBUF_READ, ex);
                }
                if ( row == null )
                    return;
                dispatch(row, changes);
            }
        } finally {
            changes.finish();
        }
    }

    /**
     * Read {@link RDF_Patch_Row} messages from {@code rows} and apply them to {@code changes}.
     *
     * @param rows iterator of {@link RDF_Patch_Row} messages
     * @param changes destination {@link RDFChanges}
     * @throws IllegalArgumentException Thrown if {@code rows} or {@code changes} is {@code null}
     */
    public static void read(Iterator<RDF_Patch_Row> rows, RDFChanges changes) {
        if ( rows == null )
            throw new IllegalArgumentException(ERR_ROWS_NULL);
        if ( changes == null )
            throw new IllegalArgumentException(ERR_CHANGES_NULL);
        changes.start();
        try {
            while ( rows.hasNext() ) {
                dispatch(rows.next(), changes);
            }
        } finally {
            changes.finish();
        }
    }

    /**
     * Apply all rows in an {@link RDF_Patch} message to {@code changes}.
     *
     * @param patch the Protobuf patch message
     * @param changes destination {@link RDFChanges}
     * @throws IllegalArgumentException Thrown if {@code patch} or {@code changes} is {@code null}
     */
    public static void read(RDF_Patch patch, RDFChanges changes) {
        if ( patch == null )
            throw new IllegalArgumentException(ERR_PATCH_NULL);
        if ( changes == null )
            throw new IllegalArgumentException(ERR_CHANGES_NULL);
        read(patch.getRowList().iterator(), changes);
    }

    /**
     * Return a {@link Consumer} of {@link RDF_Patch_Row} messages that dispatches each row to {@code changes}.
     *
     * @param changes destination {@link RDFChanges}
     * @return a consumer of {@link RDF_Patch_Row} messages
     * @throws IllegalArgumentException Thrown if {@code changes} is {@code null}
     */
    public static Consumer<RDF_Patch_Row> changesToRowConsumer(RDFChanges changes) {
        if ( changes == null )
            throw new IllegalArgumentException(ERR_CHANGES_NULL);
        return row -> dispatch(row, changes);
    }

    /**
     * Decode a single {@link RDF_Patch_Row} message and invoke the corresponding event on {@code changes}.
     *
     * @param row the Protobuf patch row message
     * @param changes destination {@link RDFChanges}
     * @throws IllegalArgumentException Thrown if {@code row} or {@code changes} is {@code null}
     * @throws PatchException Thrown if the row case is not set or contains an invalid term
     */
    public static void dispatch(RDF_Patch_Row row, RDFChanges changes) {
        if ( row == null )
            throw new IllegalArgumentException(ERR_ROW_NULL);
        if ( changes == null )
            throw new IllegalArgumentException(ERR_CHANGES_NULL);
        switch ( row.getRowCase() ) {
            case HEADER : {
                Patch_Header h = row.getHeader();
                Node n = fromProtobuf(h.getValue());
                changes.header(h.getName(), n);
                return;
            }
            case DATAADD : {
                Patch_Data_Add add = row.getDataAdd();
                Node s = fromProtobuf(add.getS());
                Node p = fromProtobuf(add.getP());
                Node o = fromProtobuf(add.getO());
                Node g = add.hasG() ? fromProtobuf(add.getG()) : null;
                changes.add(g, s, p, o);
                return;
            }
            case DATADEL : {
                Patch_Data_Del del = row.getDataDel();
                Node s = fromProtobuf(del.getS());
                Node p = fromProtobuf(del.getP());
                Node o = fromProtobuf(del.getO());
                Node g = del.hasG() ? fromProtobuf(del.getG()) : null;
                changes.delete(g, s, p, o);
                return;
            }
            case PREFIXADD : {
                Patch_Prefix_Add add = row.getPrefixAdd();
                Node gn = add.hasGraphNode() ? fromProtobuf(add.getGraphNode()) : null;
                changes.addPrefix(gn, add.getPrefix(), add.getIriStr());
                return;
            }
            case PREFIXDEL : {
                Patch_Prefix_Del del = row.getPrefixDel();
                Node gn = del.hasGraphNode() ? fromProtobuf(del.getGraphNode()) : null;
                changes.deletePrefix(gn, del.getPrefix());
                return;
            }
            case TXN : {
                PatchTxn txn = row.getTxn();
                switch ( txn ) {
                    case TX :
                        changes.txnBegin();
                        return;
                    case TC :
                        changes.txnCommit();
                        return;
                    case TA :
                        changes.txnAbort();
                        return;
                    case Segment :
                        changes.segment();
                        return;
                    case UNRECOGNIZED :
                    default :
                        throw new PatchException("Unrecognized PatchTxn: " + txn);
                }
            }
            case ROW_NOT_SET :
            default :
                throw new PatchException("Unrecognized RDF_Patch_Row: " + row);
        }
    }

    /**
     * Convert a Protobuf {@link RDF_Term} into a Jena {@link Node}.
     *
     * @param term the Protobuf term to convert
     * @return the decoded {@link Node}
     * @throws IllegalArgumentException Thrown if {@code term} is {@code null}
     * @throws PatchException Thrown if the term cannot be converted to a {@link Node}
     */
    public static Node fromProtobuf(RDF_Term term) {
        if ( term == null )
            throw new IllegalArgumentException("term must not be null");
        try {
            Node n = ProtobufConvert.convert(term);
            if ( n == null )
                throw new PatchException("No conversion to a Node: " + term);
            return n;
        } catch (RiotProtobufException ex) {
            throw new PatchException("No conversion to a Node: " + term, ex);
        }
    }
}
