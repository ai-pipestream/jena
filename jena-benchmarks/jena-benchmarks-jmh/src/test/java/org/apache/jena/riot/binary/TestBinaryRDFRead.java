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
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.jena.jmh.JmhDefaultOptions;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.lang.StreamRDFCounting;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.riot.system.StreamRDFWriter;

import org.junit.Assert;
import org.junit.Test;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

/**
 * Read speed of the binary RDF formats, RDF Thrift and RDF Protobuf.
 * <p>
 * The source file is parsed once per trial and written in the format under test to a
 * temporary file; the {@code read} benchmark then parses that file into a counting
 * sink, which measures the format's reader alone. {@link TestBinaryRDFWrite} measures
 * the writers.
 * <p>
 * Replace {@code testing/data.nt.gz} with a file of a size that gives stable numbers,
 * for example a BSBM dataset; the figure of interest is the difference between the
 * formats on the same file. The format parameter is the name of an {@link RDFFormat}
 * constant, so values that only exist on some branches ({@code RDF_PROTO_V2}) can be
 * given without changing this class.
 */
@State(Scope.Benchmark)
public class TestBinaryRDFRead {

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
    private Path file;

    private static RDFFormat getFormat(String name) {
        try {
            return (RDFFormat) RDFFormat.class.getField(name).get(null);
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("Unknown format: " + name, e);
        }
    }

    @Setup(Level.Trial)
    public void setup() throws IOException {
        format = getFormat(param1_Format);
        file = Files.createTempFile("binary-rdf-", ".bin");
        try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(file), 64 * 1024)) {
            StreamRDF writer = StreamRDFWriter.getWriterStream(out, format);
            RDFParser.source(param0_SourceFile).parse(writer);
        }
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException {
        Files.deleteIfExists(file);
    }

    @Benchmark
    public long read() {
        StreamRDFCounting sink = StreamRDFLib.count();
        RDFParser.source(file).lang(format.getLang()).parse(sink);
        return sink.count();
    }


    @Test
    public void benchmark() throws Exception {
        var opt = JmhDefaultOptions.getDefaults(this.getClass())
                .warmupIterations(2)
                .measurementIterations(5);
        // -Dbenchmark.source=<file> runs the benchmark on another file.
        String source = System.getProperty("benchmark.source");
        if ( source != null )
            opt.param("param0_SourceFile", source);
        var results = new Runner(opt.build()).run();
        Assert.assertNotNull(results);
    }
}
