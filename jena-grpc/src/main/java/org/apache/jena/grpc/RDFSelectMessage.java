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

package org.apache.jena.grpc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.ExtensionRegistryLite;

import io.grpc.MethodDescriptor;
import io.grpc.Status;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_DataTuple;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_VarTuple;

/**
 * Envelope message for streaming SPARQL {@code SELECT} results over gRPC, carrying either
 * the initial {@link RDF_VarTuple} variable header (field 1) or a subsequent
 * {@link RDF_DataTuple} binding row (field 2).
 */
public final class RDFSelectMessage {

    private static final int FIELD_HEADER = 1;
    private static final int FIELD_ROW = 2;
    private static final int TAG_HEADER = (FIELD_HEADER << 3) | 2;
    private static final int TAG_ROW = (FIELD_ROW << 3) | 2;
    private static final String ERR_HEADER_NULL = "header must not be null";
    private static final String ERR_ROW_NULL = "row must not be null";
    private static final String ERR_STREAM_NULL = "stream must not be null";

    private static final MethodDescriptor.Marshaller<RDFSelectMessage> MARSHALLER =
        new MethodDescriptor.Marshaller<>() {
            @Override
            public InputStream stream(RDFSelectMessage value) {
                try {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    CodedOutputStream cos = CodedOutputStream.newInstance(baos);
                    if ( value.header != null ) {
                        cos.writeMessage(FIELD_HEADER, value.header);
                    } else if ( value.row != null ) {
                        cos.writeMessage(FIELD_ROW, value.row);
                    }
                    cos.flush();
                    return new ByteArrayInputStream(baos.toByteArray());
                } catch (IOException ex) {
                    throw Status.INTERNAL.withDescription("Failed to serialize RDFSelectMessage").withCause(ex).asRuntimeException();
                }
            }

            @Override
            public RDFSelectMessage parse(InputStream stream) {
                if ( stream == null )
                    throw new IllegalArgumentException(ERR_STREAM_NULL);
                try {
                    CodedInputStream cis = CodedInputStream.newInstance(stream);
                    RDF_VarTuple parsedHeader = null;
                    RDF_DataTuple parsedRow = null;
                    int tag;
                    while ( (tag = cis.readTag()) != 0 ) {
                        if ( tag == TAG_HEADER ) {
                            parsedHeader = cis.readMessage(RDF_VarTuple.parser(), ExtensionRegistryLite.getEmptyRegistry());
                        } else if ( tag == TAG_ROW ) {
                            parsedRow = cis.readMessage(RDF_DataTuple.parser(), ExtensionRegistryLite.getEmptyRegistry());
                        } else {
                            cis.skipField(tag);
                        }
                    }
                    if ( parsedHeader != null )
                        return ofHeader(parsedHeader);
                    if ( parsedRow != null )
                        return ofRow(parsedRow);
                    return ofHeader(RDF_VarTuple.getDefaultInstance());
                } catch (IOException ex) {
                    throw Status.INTERNAL.withDescription("Failed to parse RDFSelectMessage").withCause(ex).asRuntimeException();
                }
            }
        };

    private final RDF_VarTuple header;
    private final RDF_DataTuple row;

    private RDFSelectMessage(RDF_VarTuple header, RDF_DataTuple row) {
        this.header = header;
        this.row = row;
    }

    /**
     * Create a message carrying the {@link RDF_VarTuple} variable header.
     *
     * @param header the variable tuple header
     * @return the wrapped {@link RDFSelectMessage}
     * @throws IllegalArgumentException Thrown if {@code header} is {@code null}
     */
    public static RDFSelectMessage ofHeader(RDF_VarTuple header) {
        if ( header == null )
            throw new IllegalArgumentException(ERR_HEADER_NULL);
        return new RDFSelectMessage(header, null);
    }

    /**
     * Create a message carrying a single {@link RDF_DataTuple} result row.
     *
     * @param row the data tuple row
     * @return the wrapped {@link RDFSelectMessage}
     * @throws IllegalArgumentException Thrown if {@code row} is {@code null}
     */
    public static RDFSelectMessage ofRow(RDF_DataTuple row) {
        if ( row == null )
            throw new IllegalArgumentException(ERR_ROW_NULL);
        return new RDFSelectMessage(null, row);
    }

    /**
     * Return the gRPC {@link MethodDescriptor.Marshaller} for {@link RDFSelectMessage}.
     *
     * @return the marshaller instance
     */
    public static MethodDescriptor.Marshaller<RDFSelectMessage> marshaller() {
        return MARSHALLER;
    }

    /**
     * Return {@code true} if this message carries an {@link RDF_VarTuple} header.
     *
     * @return {@code true} if header is present
     */
    public boolean isHeader() {
        return header != null;
    }

    /**
     * Return {@code true} if this message carries an {@link RDF_DataTuple} row.
     *
     * @return {@code true} if row is present
     */
    public boolean isRow() {
        return row != null;
    }

    /**
     * Return the {@link RDF_VarTuple} header, or {@code null} if this is a row message.
     *
     * @return the variable header or {@code null}
     */
    public RDF_VarTuple getHeader() {
        return header;
    }

    /**
     * Return the {@link RDF_DataTuple} row, or {@code null} if this is a header message.
     *
     * @return the data row or {@code null}
     */
    public RDF_DataTuple getRow() {
        return row;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object obj) {
        if ( this == obj )
            return true;
        if ( !(obj instanceof RDFSelectMessage other) )
            return false;
        return Objects.equals(header, other.header) && Objects.equals(row, other.row);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hash(header, row);
    }
}
