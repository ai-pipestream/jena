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

import java.util.function.Consumer;

import org.apache.jena.atlas.lib.Cache;
import org.apache.jena.atlas.lib.CacheFactory;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_IRI;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_PrefixDecl;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Quad;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Stream;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_StreamRow;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Triple;
import org.apache.jena.riot.system.FactoryRDFCaching;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapFactory;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;

/** Protobuf RDF (wire format items) to StreamRDF terms (Jena java objects)
 *
 * @see StreamRDF2Protobuf for the reverse process.
 */

public class Protobuf2StreamRDF implements VisitorStreamRowProtoRDF, Consumer<RDF_StreamRow> {

    private static final String ERR_PMAP_NULL = "pmap must not be null";
    private static final String ERR_STREAM_NULL = "stream must not be null";
    private static final String ERR_ROW_NULL = "row must not be null";

    private final StreamRDF dest;
    private final PrefixMap pmap;
    private final Cache<String, Node> uriCache;

    /**
     * Create a decoder writing to {@code stream} with a fresh {@link PrefixMap} and default URI cache.
     *
     * @param stream the destination {@link StreamRDF}
     * @throws IllegalArgumentException Thrown if {@code stream} is {@code null}
     */
    public Protobuf2StreamRDF(StreamRDF stream) {
        this(PrefixMapFactory.create(), stream);
    }

    /**
     * Create a decoder writing to {@code stream} with the supplied {@link PrefixMap} and default URI cache.
     *
     * @param pmap the prefix map to populate and consult
     * @param stream the destination {@link StreamRDF}
     * @throws IllegalArgumentException Thrown if {@code pmap} or {@code stream} is {@code null}
     */
    public Protobuf2StreamRDF(PrefixMap pmap, StreamRDF stream) {
        this(pmap, stream, CacheFactory.createSimpleCache(FactoryRDFCaching.DftNodeCacheSize));
    }

    /**
     * Create a decoder writing to {@code stream} with the supplied {@link PrefixMap} and optional URI cache.
     *
     * @param pmap the prefix map to populate and consult
     * @param stream the destination {@link StreamRDF}
     * @param uriCache optional cache for interning URI nodes, or {@code null} to disable caching
     * @throws IllegalArgumentException Thrown if {@code pmap} or {@code stream} is {@code null}
     */
    public Protobuf2StreamRDF(PrefixMap pmap, StreamRDF stream, Cache<String, Node> uriCache) {
        if ( pmap == null )
            throw new IllegalArgumentException(ERR_PMAP_NULL);
        if ( stream == null )
            throw new IllegalArgumentException(ERR_STREAM_NULL);
        this.pmap = pmap;
        this.dest = stream;
        this.uriCache = uriCache;
    }

    /**
     * Decode a single {@link RDF_StreamRow} and forward it to the underlying {@link StreamRDF}.
     *
     * @param row the stream row message to decode
     * @throws IllegalArgumentException Thrown if {@code row} is {@code null}
     */
    @Override
    public void accept(RDF_StreamRow row) {
        if ( row == null )
            throw new IllegalArgumentException(ERR_ROW_NULL);
        PBufRDF.visit(row, this);
    }

    /**
     * Decode all rows in an {@link RDF_Stream} batch and forward them to the underlying {@link StreamRDF}.
     *
     * @param stream the batched stream message to decode
     * @throws IllegalArgumentException Thrown if {@code stream} is {@code null}
     */
    public void accept(RDF_Stream stream) {
        if ( stream == null )
            throw new IllegalArgumentException(ERR_STREAM_NULL);
        for ( RDF_StreamRow row : stream.getRowList() ) {
            PBufRDF.visit(row, this);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void visit(RDF_Triple rt) {
        Triple t = ProtobufConvert.convert(uriCache, rt, pmap);
        dest.triple(t);
    }

    /** {@inheritDoc} */
    @Override
    public void visit(RDF_Quad rq) {
        Quad q = ProtobufConvert.convert(uriCache, rq, pmap);
        dest.quad(q);
    }

    /** {@inheritDoc} */
    @Override
    public void visit(RDF_PrefixDecl prefixDecl) {
        String prefix = prefixDecl.getPrefix();
        String iriStr = prefixDecl.getUri();
        pmap.add(prefix, iriStr);
        dest.prefix(prefix, iriStr);
    }

    /** {@inheritDoc} */
    @Override
    public void visit(RDF_IRI baseDecl) {
        String iriStr = baseDecl.getIri();
        dest.base(iriStr);
    }
}
