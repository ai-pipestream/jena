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

package org.apache.jena.riot.binary;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.jena.graph.Triple;
import org.apache.jena.jmh.JmhDefaultOptions;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFBase;
import org.apache.jena.riot.system.StreamRDFWriter;
import org.apache.jena.sys.JenaSystem;

import org.junit.Assert;
import org.junit.Test;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

/**
 * Write speed of the binary RDF formats, RDF Thrift and RDF Protobuf, with the
 * triples held in memory so that only the writer is measured. The source file is
 * parsed once per trial; each benchmark call writes every triple to a discarding
 * stream through {@link StreamRDFWriter#getWriterStream}.
 * <p>
 * The source must fit in memory as {@code Triple} objects; a few million triples
 * need a few gigabytes of heap.
 */
@State(Scope.Benchmark)
public class TestBinaryRDFWrite {

    @Param({
            "../testing/data.nt.gz",
    })
    public String param0_SourceFile;

    @Param({
            "RDF_THRIFT",
            "RDF_PROTO",
    })
    public String param1_Format;

    private RDFFormat format;
    private List<Triple> triples;

    /** See {@link TestBinaryRDFRead#getFormat}. */
    static RDFFormat getFormat(String name) {
        try {
            int hash = name.indexOf('#');
            Class<?> cls = hash < 0 ? RDFFormat.class : Class.forName(name.substring(0, hash));
            String field = hash < 0 ? name : name.substring(hash + 1);
            return (RDFFormat) cls.getField(field).get(null);
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("Unknown format: " + name, e);
        }
    }

    @Setup(Level.Trial)
    public void setup() {
        // Initialize Jena before any format constant is touched, so that a format from
        // another library can register itself with a fully built RIOT registry.
        JenaSystem.init();
        format = getFormat(param1_Format);
        List<Triple> list = new ArrayList<>();
        RDFParser.source(param0_SourceFile).parse(new StreamRDFBase() {
            @Override public void triple(Triple triple) { list.add(triple); }
        });
        triples = list;
    }

    @Benchmark
    public long write() throws IOException {
        try (OutputStream out = new BufferedOutputStream(OutputStream.nullOutputStream(), 64 * 1024)) {
            StreamRDF writer = StreamRDFWriter.getWriterStream(out, format);
            writer.start();
            for (Triple triple : triples) {
                writer.triple(triple);
            }
            writer.finish();
        }
        return triples.size();
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JmhDefaultOptions.getDefaults(this.getClass())
                .warmupIterations(2)
                .measurementIterations(5);
        // -Dbenchmark.source=<file> runs the benchmark on another file;
        // -Dbenchmark.formats=A,B,... replaces the list of formats.
        String source = System.getProperty("benchmark.source");
        if ( source != null )
            opt.param("param0_SourceFile", source);
        String formats = System.getProperty("benchmark.formats");
        if ( formats != null )
            opt.param("param1_Format", formats.split(","));
        var results = new Runner(opt.build()).run();
        Assert.assertNotNull(results);
    }
}
