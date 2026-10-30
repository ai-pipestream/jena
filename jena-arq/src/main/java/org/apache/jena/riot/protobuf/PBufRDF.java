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

import com.google.protobuf.GeneratedMessage;

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

    public static void writeDelimitedTo(GeneratedMessage gmv3, OutputStream output) {
        try {
            gmv3.writeDelimitedTo(output);
        } catch (IOException e) { IO.exception(e); }
    }

    public static RDF_Triple rdfTriple(Triple triple, RDF_Triple.Builder tripleBuilder, RDF_Term.Builder termBuilder) {
        return rdfTriple(triple, PMAP0, tripleBuilder, termBuilder, false);
    }

    /**
     * Encode a {@link Triple} into an {@link RDF_Triple} using the supplied builders, prefix map, and value encoding flag.
     *
     * @param triple the triple to encode
     * @param pmap the prefix map for URI abbreviation, or {@code null} for no abbreviation
     * @param tripleBuilder the reusable triple builder
     * @param termBuilder the reusable term builder
     * @param encodeValues {@code true} to encode numeric literals by value
     * @return the built {@link RDF_Triple}
     */
    static RDF_Triple rdfTriple(Triple triple, PrefixMap pmap, RDF_Triple.Builder tripleBuilder, RDF_Term.Builder termBuilder, boolean encodeValues) {
        tripleBuilder.clear();
        tripleBuilder.setS(rdfTerm(triple.getSubject(), pmap, termBuilder, encodeValues));
        tripleBuilder.setP(rdfTerm(triple.getPredicate(), pmap, termBuilder, encodeValues));
        tripleBuilder.setO(rdfTerm(triple.getObject(), pmap, termBuilder, encodeValues));
        return tripleBuilder.build();
    }

    public static RDF_Quad rdfQuad(Quad quad, RDF_Quad.Builder quadBuilder, RDF_Term.Builder termBuilder) {
        return rdfQuad(quad, PMAP0, quadBuilder, termBuilder, false);
    }

    /**
     * Encode a {@link Quad} into an {@link RDF_Quad} using the supplied builders, prefix map, and value encoding flag.
     *
     * @param quad the quad to encode
     * @param pmap the prefix map for URI abbreviation, or {@code null} for no abbreviation
     * @param quadBuilder the reusable quad builder
     * @param termBuilder the reusable term builder
     * @param encodeValues {@code true} to encode numeric literals by value
     * @return the built {@link RDF_Quad}
     */
    static RDF_Quad rdfQuad(Quad quad, PrefixMap pmap, RDF_Quad.Builder quadBuilder, RDF_Term.Builder termBuilder, boolean encodeValues) {
        quadBuilder.clear();
        if ( quad.getGraph() != null )
            quadBuilder.setG(rdfTerm(quad.getGraph(), pmap, termBuilder, encodeValues));
        quadBuilder.setS(rdfTerm(quad.getSubject(), pmap, termBuilder, encodeValues));
        quadBuilder.setP(rdfTerm(quad.getPredicate(), pmap, termBuilder, encodeValues));
        quadBuilder.setO(rdfTerm(quad.getObject(), pmap, termBuilder, encodeValues));
        return quadBuilder.build();
    }

    public static RDF_Term rdfTerm(Node node, RDF_Term.Builder termBuilder) {
        return rdfTerm(node, termBuilder, false);
    }

    static RDF_Term rdfTerm(Node node, RDF_Term.Builder termBuilder, boolean encodeValues) {
        return rdfTerm(node, PMAP0, termBuilder, encodeValues);
    }

    /**
     * Encode a {@link Node} into an {@link RDF_Term} using the supplied term builder, prefix map, and value encoding flag.
     *
     * @param node the node to encode
     * @param pmap the prefix map for URI abbreviation, or {@code null} for no abbreviation
     * @param termBuilder the reusable term builder
     * @param encodeValues {@code true} to encode numeric literals by value
     * @return the built {@link RDF_Term}
     */
    static RDF_Term rdfTerm(Node node, PrefixMap pmap, RDF_Term.Builder termBuilder, boolean encodeValues) {
        termBuilder.clear();
        PrefixMap activeMap = (pmap != null) ? pmap : PMAP0;
        return ProtobufConvert.toProtobuf(node, activeMap, termBuilder, encodeValues);
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
