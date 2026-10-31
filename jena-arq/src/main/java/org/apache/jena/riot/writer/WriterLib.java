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

package org.apache.jena.riot.writer;

import org.apache.jena.atlas.lib.CharSpace;
import org.apache.jena.riot.RIOT;
import org.apache.jena.riot.out.NodeFormatter;
import org.apache.jena.riot.out.NodeFormatterNT;
import org.apache.jena.riot.out.NodeToLabel;
import org.apache.jena.sparql.util.Context;

/** Package-scoped utilities */
/*package*/ class WriterLib {

    private static final IndentStyle dftIndentStyle = IndentStyle.WIDE;

    // Determine the directive style (applies to PREFIX and BASE).
    /*package*/ static DirectiveStyle directiveStyle(Context context) {
        if ( context == null )
            return DirectiveStyle.systemDefault;

        Object x = context.get(RIOT.symTurtleDirectiveStyle);
        if ( x instanceof String s )
            return DirectiveStyle.create(s);
        if ( x instanceof DirectiveStyle directive)
            return directive;

        // Default choice; includes null in context.
        return DirectiveStyle.systemDefault;
    }

    /*package*/ static IndentStyle indentStyle(Context context) {
        if ( context == null )
            return dftIndentStyle;
        Object x = context.get(RIOT.symTurtleIndentStyle);

        if ( x instanceof String s ) {
            IndentStyle style = IndentStyle.create(s);
            return style == null ? dftIndentStyle : style;
        }

        // Default choice; includes null in context.
        return dftIndentStyle;
    }

    /** Extract a configured {@link NodeFormatter} from the {@link Context}, or return {@code null}. */
    /*package*/ static NodeFormatter nodeFormatter(Context context) {
        if ( context == null )
            return null;
        Object x = context.get(RIOT.symNodeFormatter);
        if ( x instanceof NodeFormatter fmt )
            return fmt;
        return null;
    }

    /** Extract a configured {@link NodeToLabel} from the {@link Context}, or return {@code null}. */
    /*package*/ static NodeToLabel nodeToLabel(Context context) {
        if ( context == null )
            return null;
        Object x = context.get(RIOT.symNodeToLabel);
        if ( x instanceof NodeToLabel n2l )
            return n2l;
        return null;
    }

    /** Return true if a custom {@link NodeFormatter} or {@link NodeToLabel} is configured in the {@link Context}. */
    /*package*/ static boolean hasCustomNodeOutput(Context context) {
        return nodeFormatter(context) != null || nodeToLabel(context) != null;
    }

    /** Create a {@link NodeFormatter} for N-Triples/N-Quads taking {@link Context} settings into account. */
    /*package*/ static NodeFormatter nodeFormatterNT(CharSpace charSpace, Context context) {
        NodeFormatter fmt = nodeFormatter(context);
        if ( fmt != null )
            return fmt;
        NodeToLabel nodeToLabel = nodeToLabel(context);
        return new NodeFormatterNT(charSpace, nodeToLabel);
    }
}
