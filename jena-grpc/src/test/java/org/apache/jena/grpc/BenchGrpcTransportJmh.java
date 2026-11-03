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

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.grpc.BenchGrpcTransport.ListGraph;
import org.apache.jena.grpc.BenchGrpcTransport.Target;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.system.StreamRDFBase;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.DatasetGraphSink;
import org.apache.jena.sys.JenaSystem;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

/**
 * JMH benchmarks for the RDF transfer calls of the jena-grpc service: the
 * server-streaming {@code StreamRdf}, the unary {@code GetRdf} and the bidirectional
 * {@code RdfSession} for reads; the client-streaming {@code WriteRdf}, the unary
 * {@code PutRdf} and {@code RdfSession} for writes; and four ways of writing many
 * small documents. Server and client run in one JVM over loopback Netty or the
 * in-process transport. Reads come from a list-backed graph into a counting sink
 * and writes go into a dataset that discards what it receives, so the figures are
 * transport plus encoding and decoding, without a store.
 * <p>
 * The source file is parsed into memory once per trial. Each timed operation is one
 * complete transfer of all its triples. The document operations write
 * {@code param3_Documents} graphs of {@code param4_DocumentSize} triples each, taken
 * from the start of the same data, one write per document.
 */
@State(Scope.Benchmark)
public class BenchGrpcTransportJmh {

    @Param({ "../jena-benchmarks/testing/data.nt.gz" })
    public String param0_SourceFile;

    @Param({ "netty", "inprocess" })
    public String param1_Transport;

    @Param({ "v2", "v1" })
    public String param2_Payload;

    @Param({ "10000" })
    public int param3_Documents;

    @Param({ "100" })
    public int param4_DocumentSize;

    private List<Triple> triples;
    private Graph source;
    private List<Graph> documents;
    private Target read;
    private Target write;
    private int count;

    @Setup(Level.Trial)
    public void setup() {
        JenaSystem.init();
        List<Triple> list = new ArrayList<>();
        RDFParser.source(param0_SourceFile).lang(Lang.NTRIPLES).parse(new StreamRDFBase() {
            @Override public void triple(Triple t) { list.add(t); }
        });
        triples = list;
        count = list.size();
        source = new ListGraph(list);
        documents = new ArrayList<>(param3_Documents);
        int docs = Math.min(param3_Documents, list.size() / param4_DocumentSize);
        for ( int d = 0; d < docs; d++ ) {
            int from = d * param4_DocumentSize;
            documents.add(new ListGraph(list.subList(from, Math.min(from + param4_DocumentSize, list.size()))));
        }
        boolean termTables = param2_Payload.equals("v2");
        boolean netty = param1_Transport.equals("netty");
        DatasetGraph readDsg = DatasetGraphFactory.wrap(source);
        DatasetGraph sink = DatasetGraphSink.create();
        read = Target.start(readDsg, termTables, netty);
        write = Target.start(sink, termTables, netty);
    }

    @TearDown(Level.Trial)
    public void teardown() {
        read.close();
        write.close();
    }

    private boolean termTables() { return param2_Payload.equals("v2"); }

    @Benchmark public void read_streamRdf()   { BenchGrpcTransport.streamRdf(read, count); }
    @Benchmark public void read_getRdf()      { BenchGrpcTransport.getRdf(read, count); }
    @Benchmark public void read_session() throws Exception { BenchGrpcTransport.sessionRead(read, count); }

    @Benchmark public void write_writeRdf()   { BenchGrpcTransport.writeRdf(write, source, termTables()); }
    @Benchmark public void write_putRdf()     { BenchGrpcTransport.putRdf(write, source, termTables()); }
    @Benchmark public void write_session() throws Exception { BenchGrpcTransport.sessionWrite(write, source, termTables()); }

    @Benchmark public void documents_putRdf()            { BenchGrpcTransport.docsUnary(write, documents); }
    @Benchmark public void documents_writeRdf()          { BenchGrpcTransport.docsWriteRdf(write, documents); }
    @Benchmark public void documents_sessionWaitEach() throws Exception { BenchGrpcTransport.docsSession(write, documents, false); }
    @Benchmark public void documents_sessionPipelined() throws Exception { BenchGrpcTransport.docsSession(write, documents, true); }

    /**
     * Runs the benchmarks under Maven: {@code mvn -pl jena-grpc -Dtest=BenchGrpcTransportJmh
     * -Dbenchmark.skip=false test}. Skipped unless {@code benchmark.skip} is set to false, so
     * an ordinary build does not run it. {@code -Dbenchmark.source=<file>} sets the data file,
     * {@code -Dbenchmark.transport=netty|inprocess}, {@code -Dbenchmark.payload=v2|v1} and
     * {@code -Dbenchmark.include=<regex>} narrow the run; the JSON result goes to
     * {@code target/BenchGrpcTransportJmh.json}.
     */
    @Test
    public void benchmark() throws Exception {
        Assumptions.assumeTrue("false".equals(System.getProperty("benchmark.skip", "true")),
                               "JMH benchmark: run with -Dbenchmark.skip=false");
        String include = System.getProperty("benchmark.include", ".*");
        var opt = new OptionsBuilder()
            .include(BenchGrpcTransportJmh.class.getName() + "\\." + include)
            .mode(Mode.AverageTime)
            .timeUnit(java.util.concurrent.TimeUnit.SECONDS)
            .warmupTime(TimeValue.NONE).warmupIterations(2)
            .measurementTime(TimeValue.NONE).measurementIterations(5)
            .threads(1).forks(1).shouldFailOnError(true).shouldDoGC(true)
            .jvmArgs("-Xmx8g", "-XX:+UseParallelGC")
            .resultFormat(ResultFormatType.JSON).result("target/BenchGrpcTransportJmh.json");
        param(opt, "param0_SourceFile", "benchmark.source");
        param(opt, "param1_Transport", "benchmark.transport");
        param(opt, "param2_Payload", "benchmark.payload");
        new Runner(opt.build()).run();
    }

    private static void param(ChainedOptionsBuilder opt, String param, String property) {
        String value = System.getProperty(property);
        if ( value != null )
            opt.param(param, value.split(","));
    }
}
