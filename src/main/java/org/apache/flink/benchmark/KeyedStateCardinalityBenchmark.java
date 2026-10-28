/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.benchmark;

import org.apache.flink.benchmark.functions.IntLongApplications;
import org.apache.flink.benchmark.functions.IntegerLongSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.contrib.streaming.state.RocksDBOptions;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.FileUtils;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.File;
import java.nio.file.Files;
import java.time.Duration;

import static org.openjdk.jmh.annotations.Scope.Thread;

/**
 * Keyed-state throughput as a function of key cardinality, measured on the state backend that the
 * benchmark actually selects.
 *
 * <p>The two official state-backend benchmarks cannot answer this question.
 * StateBackendBenchmarkBase.StateBackendContext.setUp() builds a {@code Configuration} that it
 * never applies: it is a local variable that goes out of scope, so the {@code state.backend} and
 * {@code state.checkpoint-storage} values it sets have no effect. FlinkEnvironmentContext.setUp()
 * separately force-sets {@code state.backend=hashmap} on the cluster configuration. Every leg of
 * the official state group therefore runs HashMapStateBackend, and the spread reported between the
 * MEMORY/FS/ROCKS/ROCKS_INC parameters is run-to-run noise rather than state-backend cost.
 *
 * <p>Rather than editing those upstream files, which would cost the fork its mergeability, this
 * class overrides the backend at job level. StateBackendLoader merges the cluster configuration
 * first and the job configuration over it, and the environment's configuration is what reaches the
 * job graph, so {@code env.configure()} wins over the cluster-level hashmap default. Enabling a
 * checkpoint interval that no job run reaches is also required: the backend is only resolved from
 * configuration when the job graph carries checkpointing settings.
 *
 * <p>A guard keeps a dead configuration from silently producing numbers again: the selected backend
 * is asserted against the effective job configuration and against the class that StateBackendLoader
 * resolves from it, which is the same code path the job manager and the task take. Asserting on the
 * resolved class rather than on RocksDB's instance directory matters, because the directory is
 * deleted when the job finishes and is therefore always empty by teardown.
 *
 * <p>Checkpointing stays disabled, so no snapshot is taken and the sweep isolates per-record state
 * access. RocksDB still writes its own SST files while compacting, which is part of that backend's
 * cost and is reported as such. The cardinality points cross the working-set size of the last-level
 * cache, which is the knee that proposal section 3.4 asks about.
 */
@OperationsPerInvocation(value = KeyedStateCardinalityBenchmark.RECORDS_PER_INVOCATION)
public class KeyedStateCardinalityBenchmark extends BenchmarkBase {

    /**
     * Records per benchmark invocation. This follows RocksStateBackendBenchmark rather than
     * MemoryStateBackendBenchmark, whose 7_000_000 makes the two official groups incomparable.
     */
    public static final int RECORDS_PER_INVOCATION = 2_000_000;

    public static final String HASHMAP = "hashmap";
    public static final String ROCKSDB = "rocksdb";

    public static void main(String[] args) throws RunnerException {
        Options options =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(
                                ".*"
                                        + KeyedStateCardinalityBenchmark.class.getCanonicalName()
                                        + ".*")
                        .build();

        new Runner(options).run();
    }

    @Benchmark
    public void keyedState(CardinalityContext context) throws Exception {
        IntLongApplications.reduceWithWindow(
                context.source, TumblingEventTimeWindows.of(Duration.ofSeconds(10_000)));
        context.execute();
    }

    /** MiniCluster environment with an enforced backend and a swept key cardinality. */
    @State(Thread)
    public static class CardinalityContext extends FlinkEnvironmentContext {

        @Param({"1", "1000", "100000", "1000000"})
        public int numberOfKeys = 1;

        @Param({KeyedStateCardinalityBenchmark.HASHMAP, KeyedStateCardinalityBenchmark.ROCKSDB})
        public String stateBackend = HASHMAP;

        public DataStreamSource<IntegerLongSource.Record> source;

        private File rocksDbDir;

        @Override
        public void setUp() throws Exception {
            super.setUp();

            // DefaultExecutionGraphBuilder only resolves a state backend from configuration inside
            // the "checkpointing enabled" branch, where enabled means the job graph carries
            // checkpointing settings. An interval far longer than any job run injects no barrier
            // (and ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH is already false), so this changes nothing
            // in the measured path while making state.backend take effect at all. Without it every
            // leg silently runs the cluster default, which is how the official state group measured
            // HashMapStateBackend four times over.
            env.enableCheckpointing(Duration.ofHours(1).toMillis());

            final Configuration jobConfig = new Configuration();
            switch (stateBackend) {
                case HASHMAP:
                    jobConfig.set(StateBackendOptions.STATE_BACKEND, HASHMAP);
                    break;
                case ROCKSDB:
                    jobConfig.set(StateBackendOptions.STATE_BACKEND, ROCKSDB);
                    // The same explicit managed-memory bound the official RocksDB benchmark sets:
                    // the default for a 1 GB task manager with one slot.
                    jobConfig.set(
                            RocksDBOptions.FIX_PER_SLOT_MEMORY_SIZE,
                            MemorySize.parse("322122552b"));
                    rocksDbDir = Files.createTempDirectory("bench-rocks-").toFile();
                    jobConfig.set(RocksDBOptions.LOCAL_DIRECTORIES, rocksDbDir.getAbsolutePath());
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unknown state backend: " + stateBackend);
            }
            env.configure(jobConfig);

            // Fail loudly instead of silently measuring the wrong backend, as the official state
            // group does.
            final String effective =
                    env.getConfiguration()
                            .getOptional(StateBackendOptions.STATE_BACKEND)
                            .orElse("");
            if (!stateBackend.equals(effective)) {
                throw new IllegalStateException(
                        "state.backend did not reach the job configuration: requested="
                                + stateBackend
                                + " effective="
                                + effective);
            }
            final String expectedClass =
                    ROCKSDB.equals(stateBackend)
                            ? "EmbeddedRocksDBStateBackend"
                            : "HashMapStateBackend";
            final StateBackend resolved;
            try {
                resolved =
                        StateBackendLoader.fromApplicationOrConfigOrDefault(
                                null, jobConfig, jobConfig, getClass().getClassLoader(), null);
            } catch (Exception e) {
                throw new IllegalStateException("state backend resolution failed", e);
            }
            if (!expectedClass.equals(resolved.getClass().getSimpleName())) {
                throw new IllegalStateException(
                        "state.backend="
                                + stateBackend
                                + " resolved to "
                                + resolved.getClass().getName()
                                + ", expected "
                                + expectedClass);
            }

            source = env.addSource(new IntegerLongSource(numberOfKeys, RECORDS_PER_INVOCATION));
        }

        @Override
        public void tearDown() throws Exception {
            super.tearDown();
            if (rocksDbDir != null) {
                FileUtils.deleteDirectory(rocksDbDir);
                rocksDbDir = null;
            }
        }
    }
}
