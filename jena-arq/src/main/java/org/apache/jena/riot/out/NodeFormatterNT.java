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

package org.apache.jena.riot.out;

import org.apache.jena.atlas.io.AWriter;
import org.apache.jena.atlas.lib.CharSpace;
import org.apache.jena.atlas.lib.EscapeStr;
import org.apache.jena.graph.Node;
import org.apache.jena.riot.out.quoted.QuotedStringOutput;
import org.apache.jena.riot.out.quoted.QuotedStringOutputNT;
import org.apache.jena.riot.out.quoted.QuotedURI;

public class NodeFormatterNT extends NodeFormatterBase
{
    // Formatting for NTriples
    // Turtles extends this class to intercept forms it can do better.

    private final QuotedStringOutput quotedStringProc;
    private final QuotedURI quotedUriProc;
    private final NodeToLabel nodeToLabel;

    public NodeFormatterNT() { this(CharSpace.UTF8, null); }

    public NodeFormatterNT(CharSpace charSpace) { this(charSpace, null); }

    /**
     * Create a formatter for N-Triples/N-Quads using UTF-8 and the given {@link NodeToLabel} policy.
     *
     * @param nodeToLabel Policy for mapping blank nodes to labels, or {@code null} for default encoding.
     */
    public NodeFormatterNT(NodeToLabel nodeToLabel) { this(CharSpace.UTF8, nodeToLabel); }

    /**
     * Create a formatter for N-Triples/N-Quads with the given {@link CharSpace} and {@link NodeToLabel} policy.
     *
     * @param charSpace Character space (UTF-8 or ASCII).
     * @param nodeToLabel Policy for mapping blank nodes to labels, or {@code null} for default encoding.
     */
    public NodeFormatterNT(CharSpace charSpace, NodeToLabel nodeToLabel) {
        quotedStringProc = new QuotedStringOutputNT(charSpace);
        quotedUriProc = new QuotedURI(charSpace);
        this.nodeToLabel = nodeToLabel;
    }

    @Override
    public void formatURI(AWriter w, String uriStr) {
        quotedUriProc.writeURI(w, uriStr);
    }

    @Override
    public void formatVar(AWriter w, String name) {
        w.print('?');
        EscapeStr.stringEsc(w, name, false);
    }

    @Override
    public void formatBNode(AWriter w, Node n) {
        if ( nodeToLabel != null ) {
            String x = nodeToLabel.get(null, n);
            w.print(x);
            return;
        }
        super.formatBNode(w, n);
    }

    @Override
    public void formatBNode(AWriter w, String label) {
        w.print("_:");
        String lab = NodeFmtLib.encodeBNodeLabel(label);
        w.print(lab);
    }

    @Override
    public void formatLitString(AWriter w, String lex) {
        writeEscaped(w, lex);
    }

    private void writeEscaped(AWriter w, String lex) {
        quotedStringProc.writeStr(w, lex);
    }

    @Override
    public void formatLitLang(AWriter w, String lex, String langTag) {
        writeEscaped(w, lex);
        w.print('@');
        w.print(langTag);
    }

    @Override
    public void formatLitLangDir(AWriter w, String lex, String langTag, String direction) {
        writeEscaped(w, lex);
        w.print('@');
        w.print(langTag);
        w.print("--");
        w.print(direction);
    }

    @Override
    public void formatLitDT(AWriter w, String lex, String datatypeURI) {
        writeEscaped(w, lex);
        w.print("^^");
        formatURI(w, datatypeURI);
    }
}
