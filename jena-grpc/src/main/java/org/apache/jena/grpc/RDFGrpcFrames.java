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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;
import com.google.protobuf.WireFormat;
import io.grpc.Drainable;
import io.grpc.KnownLength;
import io.grpc.MethodDescriptor.Marshaller;
import io.grpc.Status;
import org.apache.jena.grpc.wire.PB_RDFService.StreamRdfRequest;
import org.apache.jena.grpc.wire.PB_RDFService.WriteRdfHeader;
import org.apache.jena.grpc.wire.PB_RDFService.WriteRdfResponse;

/**
 * The messages that carry RDF rows, as the client and server handle them. On the wire they
 * are exactly the generated messages of {@code rdf_service.proto}; in Java the rows stay as
 * a frame, the bytes of one {@code RDF_Stream} message, so that they go straight between
 * the transport and the RDF Protobuf stream reader and writer without building a message
 * object per row and per term. The fields around the rows (headers, operation ids, flags)
 * are read and written by hand and the nested non-RDF messages use their generated classes.
 * <p>
 * A frame is produced by {@link org.apache.jena.riot.protobuf.ProtobufRDF#streamToFrames}
 * and decoded by {@link org.apache.jena.riot.protobuf.ProtobufRDF#framesToStreamRDF}; any
 * other client can parse it with {@code RDF_Stream.parseFrom}. The frames of one RDF stream
 * must be decoded in order by one decoder.
 */
public final class RDFGrpcFrames {

    private static final int LEN = WireFormat.WIRETYPE_LENGTH_DELIMITED;
    private static final int VARINT = WireFormat.WIRETYPE_VARINT;
    private static final ByteBuffer NO_ROWS = ByteBuffer.allocate(0);

    private RDFGrpcFrames() {}

    /** {@code StreamRdfResponse}: a batch of rows. The message is the frame. */
    public record StreamRdfFrame(ByteBuffer rows) {
        public StreamRdfFrame {
            Objects.requireNonNull(rows);
        }
    }

    /** {@code GetRdfResponse}: all rows of the selection in one frame. */
    public record GetRdfFrame(ByteBuffer rows) {
        public GetRdfFrame {
            Objects.requireNonNull(rows);
        }
    }

    /**
     * {@code WriteRdfRequest}: the header frame of a write, or a frame of rows. A
     * single-row message from another client arrives as a frame of one row.
     */
    public record WriteRdfFrame(WriteRdfHeader header, ByteBuffer rows) {
        public static WriteRdfFrame header(WriteRdfHeader header) {
            return new WriteRdfFrame(Objects.requireNonNull(header), null);
        }

        public static WriteRdfFrame rows(ByteBuffer rows) {
            return new WriteRdfFrame(null, Objects.requireNonNull(rows));
        }
    }

    /** {@code PutRdfRequest}: the header and all rows of a write in one message; {@code rows} may be {@code null}. */
    public record PutRdfFrame(WriteRdfHeader header, ByteBuffer rows) {
        public PutRdfFrame {
            Objects.requireNonNull(header);
        }
    }

    /** {@code RdfSessionRequest}: one operation frame of a session; exactly one of the four parts is set. */
    public record SessionRequest(long opId, StreamRdfRequest read, WriteRdfHeader write, ByteBuffer writeRows, boolean writeEnd) {
        public static SessionRequest read(long opId, StreamRdfRequest read) {
            return new SessionRequest(opId, Objects.requireNonNull(read), null, null, false);
        }

        public static SessionRequest write(long opId, WriteRdfHeader write) {
            return new SessionRequest(opId, null, Objects.requireNonNull(write), null, false);
        }

        public static SessionRequest writeRows(long opId, ByteBuffer rows) {
            return new SessionRequest(opId, null, null, Objects.requireNonNull(rows), false);
        }

        public static SessionRequest writeEnd(long opId) {
            return new SessionRequest(opId, null, null, null, true);
        }
    }

    /** {@code RdfSessionResponse}: one event of a session operation; exactly one of the three parts is set. */
    public record SessionResponse(long opId, ByteBuffer readRows, boolean readEnd, WriteRdfResponse writeDone) {
        public static SessionResponse readRows(long opId, ByteBuffer rows) {
            return new SessionResponse(opId, Objects.requireNonNull(rows), false, null);
        }

        public static SessionResponse readEnd(long opId) {
            return new SessionResponse(opId, null, true, null);
        }

        public static SessionResponse writeDone(long opId, WriteRdfResponse done) {
            return new SessionResponse(opId, null, false, Objects.requireNonNull(done));
        }
    }

    // ---- Marshallers

    static final Marshaller<StreamRdfFrame> STREAM_RDF_RESPONSE = new Marshaller<>() {
        @Override
        public InputStream stream(StreamRdfFrame value) {
            return new FrameInputStream(null, value.rows());
        }

        @Override
        public StreamRdfFrame parse(InputStream stream) {
            // repeated RDF_StreamRow rows = 1: the message and an RDF_Stream are the same bytes.
            return new StreamRdfFrame(ByteBuffer.wrap(readAll(stream)));
        }
    };

    static final Marshaller<GetRdfFrame> GET_RDF_RESPONSE = new Marshaller<>() {
        @Override
        public InputStream stream(GetRdfFrame value) {
            return new FrameInputStream(frameField(1, value.rows()), value.rows());
        }

        @Override
        public GetRdfFrame parse(InputStream stream) {
            byte[] bytes = readAll(stream);
            ByteBuffer rows = NO_ROWS;
            try {
                CodedInputStream in = CodedInputStream.newInstance(bytes);
                for ( int tag = in.readTag(); tag != 0; tag = in.readTag() ) {
                    if ( tag == tag(1, LEN) )
                        rows = slice(bytes, in);
                    else
                        in.skipField(tag);
                }
            } catch (IOException ex) { throw malformed(ex); }
            return new GetRdfFrame(rows);
        }
    };

    static final Marshaller<WriteRdfFrame> WRITE_RDF_REQUEST = new Marshaller<>() {
        @Override
        public InputStream stream(WriteRdfFrame value) {
            if ( value.header() != null )
                return new FrameInputStream(messageField(1, value.header()), null);
            return new FrameInputStream(frameField(2, value.rows()), value.rows());
        }

        @Override
        public WriteRdfFrame parse(InputStream stream) {
            byte[] bytes = readAll(stream);
            WriteRdfHeader header = null;
            ByteBuffer rows = null;
            try {
                CodedInputStream in = CodedInputStream.newInstance(bytes);
                for ( int tag = in.readTag(); tag != 0; tag = in.readTag() ) {
                    if ( tag == tag(1, LEN) )
                        header = WriteRdfHeader.parseFrom(slice(bytes, in));
                    else if ( tag == tag(2, LEN) )
                        rows = slice(bytes, in);
                    else if ( tag == tag(3, LEN) )
                        rows = singleRowFrame(slice(bytes, in));
                    else
                        in.skipField(tag);
                }
            } catch (IOException ex) { throw malformed(ex); }
            if ( header != null )
                return WriteRdfFrame.header(header);
            // A message with nothing set is an empty batch.
            return WriteRdfFrame.rows(rows != null ? rows : NO_ROWS);
        }
    };

    static final Marshaller<PutRdfFrame> PUT_RDF_REQUEST = new Marshaller<>() {
        @Override
        public InputStream stream(PutRdfFrame value) {
            byte[] header = messageField(1, value.header());
            if ( value.rows() == null )
                return new FrameInputStream(header, null);
            return new FrameInputStream(concat(header, frameField(2, value.rows())), value.rows());
        }

        @Override
        public PutRdfFrame parse(InputStream stream) {
            byte[] bytes = readAll(stream);
            WriteRdfHeader header = WriteRdfHeader.getDefaultInstance();
            ByteBuffer rows = null;
            try {
                CodedInputStream in = CodedInputStream.newInstance(bytes);
                for ( int tag = in.readTag(); tag != 0; tag = in.readTag() ) {
                    if ( tag == tag(1, LEN) )
                        header = WriteRdfHeader.parseFrom(slice(bytes, in));
                    else if ( tag == tag(2, LEN) )
                        rows = slice(bytes, in);
                    else
                        in.skipField(tag);
                }
            } catch (IOException ex) { throw malformed(ex); }
            return new PutRdfFrame(header, rows);
        }
    };

    static final Marshaller<SessionRequest> SESSION_REQUEST = new Marshaller<>() {
        @Override
        public InputStream stream(SessionRequest value) {
            if ( value.writeRows() != null ) {
                byte[] prefix = concat(varintField(1, value.opId()), frameField(4, value.writeRows()));
                return new FrameInputStream(prefix, value.writeRows());
            }
            byte[] rest;
            if ( value.read() != null )
                rest = messageField(2, value.read());
            else if ( value.write() != null )
                rest = messageField(3, value.write());
            else
                rest = varintField(5, 1);
            return new FrameInputStream(concat(varintField(1, value.opId()), rest), null);
        }

        @Override
        public SessionRequest parse(InputStream stream) {
            byte[] bytes = readAll(stream);
            long opId = 0;
            StreamRdfRequest read = null;
            WriteRdfHeader write = null;
            ByteBuffer writeRows = null;
            boolean writeEnd = false;
            try {
                CodedInputStream in = CodedInputStream.newInstance(bytes);
                for ( int tag = in.readTag(); tag != 0; tag = in.readTag() ) {
                    if ( tag == tag(1, VARINT) )
                        opId = in.readUInt64();
                    else if ( tag == tag(2, LEN) )
                        read = StreamRdfRequest.parseFrom(slice(bytes, in));
                    else if ( tag == tag(3, LEN) )
                        write = WriteRdfHeader.parseFrom(slice(bytes, in));
                    else if ( tag == tag(4, LEN) )
                        writeRows = slice(bytes, in);
                    else if ( tag == tag(5, VARINT) )
                        writeEnd = in.readBool();
                    else
                        in.skipField(tag);
                }
            } catch (IOException ex) { throw malformed(ex); }
            return new SessionRequest(opId, read, write, writeRows, writeEnd);
        }
    };

    static final Marshaller<SessionResponse> SESSION_RESPONSE = new Marshaller<>() {
        @Override
        public InputStream stream(SessionResponse value) {
            if ( value.readRows() != null ) {
                byte[] prefix = concat(varintField(1, value.opId()), frameField(2, value.readRows()));
                return new FrameInputStream(prefix, value.readRows());
            }
            byte[] rest = value.writeDone() != null ? messageField(4, value.writeDone()) : varintField(3, 1);
            return new FrameInputStream(concat(varintField(1, value.opId()), rest), null);
        }

        @Override
        public SessionResponse parse(InputStream stream) {
            byte[] bytes = readAll(stream);
            long opId = 0;
            ByteBuffer readRows = null;
            boolean readEnd = false;
            WriteRdfResponse writeDone = null;
            try {
                CodedInputStream in = CodedInputStream.newInstance(bytes);
                for ( int tag = in.readTag(); tag != 0; tag = in.readTag() ) {
                    if ( tag == tag(1, VARINT) )
                        opId = in.readUInt64();
                    else if ( tag == tag(2, LEN) )
                        readRows = slice(bytes, in);
                    else if ( tag == tag(3, VARINT) )
                        readEnd = in.readBool();
                    else if ( tag == tag(4, LEN) )
                        writeDone = WriteRdfResponse.parseFrom(slice(bytes, in));
                    else
                        in.skipField(tag);
                }
            } catch (IOException ex) { throw malformed(ex); }
            return new SessionResponse(opId, readRows, readEnd, writeDone);
        }
    };

    // ---- Wire helpers

    private static int tag(int field, int wireType) {
        return (field << 3) | wireType;
    }

    /** The bytes of the length-delimited field the stream is positioned at, as a buffer over the message bytes. */
    private static ByteBuffer slice(byte[] bytes, CodedInputStream in) throws IOException {
        int length = in.readRawVarint32();
        int offset = in.getTotalBytesRead();
        in.skipRawBytes(length);
        return ByteBuffer.wrap(bytes, offset, length).slice();
    }

    /** A frame holding one row, from the bytes of an {@code RDF_StreamRow}. */
    private static ByteBuffer singleRowFrame(ByteBuffer row) {
        int length = row.remaining();
        byte[] frame = new byte[CodedOutputStream.computeTagSize(1) + CodedOutputStream.computeUInt32SizeNoTag(length) + length];
        try {
            CodedOutputStream out = CodedOutputStream.newInstance(frame);
            out.writeTag(1, LEN);
            out.writeUInt32NoTag(length);
            out.write(row.duplicate());
            out.flush();
        } catch (IOException ex) { throw malformed(ex); }
        return ByteBuffer.wrap(frame);
    }

    /** The tag and length of a length-delimited field whose bytes are the frame; the frame itself follows separately. */
    private static byte[] frameField(int field, ByteBuffer frame) {
        int length = frame.remaining();
        byte[] prefix = new byte[CodedOutputStream.computeTagSize(field) + CodedOutputStream.computeUInt32SizeNoTag(length)];
        try {
            CodedOutputStream out = CodedOutputStream.newInstance(prefix);
            out.writeTag(field, LEN);
            out.writeUInt32NoTag(length);
            out.flush();
        } catch (IOException ex) { throw malformed(ex); }
        return prefix;
    }

    private static byte[] messageField(int field, MessageLite message) {
        byte[] bytes = new byte[CodedOutputStream.computeMessageSize(field, message)];
        try {
            CodedOutputStream out = CodedOutputStream.newInstance(bytes);
            out.writeMessage(field, message);
            out.flush();
        } catch (IOException ex) { throw malformed(ex); }
        return bytes;
    }

    private static byte[] varintField(int field, long value) {
        byte[] bytes = new byte[CodedOutputStream.computeUInt64Size(field, value)];
        try {
            CodedOutputStream out = CodedOutputStream.newInstance(bytes);
            out.writeUInt64(field, value);
            out.flush();
        } catch (IOException ex) { throw malformed(ex); }
        return bytes;
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] c = new byte[a.length + b.length];
        System.arraycopy(a, 0, c, 0, a.length);
        System.arraycopy(b, 0, c, a.length, b.length);
        return c;
    }

    private static byte[] readAll(InputStream stream) {
        try {
            if ( stream instanceof KnownLength ) {
                byte[] bytes = new byte[stream.available()];
                int n = stream.readNBytes(bytes, 0, bytes.length);
                if ( n != bytes.length )
                    throw new IOException("Short read: " + n + " of " + bytes.length + " bytes");
                return bytes;
            }
            return stream.readAllBytes();
        } catch (IOException ex) { throw malformed(ex); }
    }

    private static RuntimeException malformed(IOException ex) {
        return Status.INTERNAL.withDescription("Malformed RDF message: " + ex.getMessage()).withCause(ex).asRuntimeException();
    }

    /** A message as an input stream: a prefix of hand-written fields, then the frame bytes, either of which may be absent. */
    private static final class FrameInputStream extends InputStream implements KnownLength, Drainable {
        private final byte[] prefix;
        private final byte[] frame;
        private final int frameOffset;
        private final int frameEnd;
        private int position = 0;    // Position in the concatenation of prefix and frame.
        private final int length;

        FrameInputStream(byte[] prefix, ByteBuffer frame) {
            this.prefix = prefix != null ? prefix : new byte[0];
            if ( frame != null ) {
                if ( !frame.hasArray() )
                    throw new IllegalArgumentException("Frame must be a heap buffer");
                this.frame = frame.array();
                this.frameOffset = frame.arrayOffset() + frame.position();
                this.frameEnd = frame.arrayOffset() + frame.limit();
            } else {
                this.frame = new byte[0];
                this.frameOffset = 0;
                this.frameEnd = 0;
            }
            this.length = this.prefix.length + (frameEnd - frameOffset);
        }

        @Override
        public int read() {
            if ( position >= length )
                return -1;
            int b = position < prefix.length
                ? prefix[position]
                : frame[frameOffset + position - prefix.length];
            position++;
            return b & 0xFF;
        }

        @Override
        public int read(byte[] dest, int off, int len) {
            if ( position >= length )
                return -1;
            int n = Math.min(len, length - position);
            int copied = 0;
            if ( position < prefix.length ) {
                int fromPrefix = Math.min(n, prefix.length - position);
                System.arraycopy(prefix, position, dest, off, fromPrefix);
                position += fromPrefix;
                copied = fromPrefix;
            }
            if ( copied < n ) {
                int fromFrame = n - copied;
                System.arraycopy(frame, frameOffset + position - prefix.length, dest, off + copied, fromFrame);
                position += fromFrame;
            }
            return n;
        }

        @Override
        public int available() {
            return length - position;
        }

        @Override
        public int drainTo(OutputStream target) throws IOException {
            int n = available();
            if ( position < prefix.length ) {
                target.write(prefix, position, prefix.length - position);
                position = prefix.length;
            }
            int frameStart = frameOffset + position - prefix.length;
            if ( frameStart < frameEnd )
                target.write(frame, frameStart, frameEnd - frameStart);
            position = length;
            return n;
        }
    }
}
