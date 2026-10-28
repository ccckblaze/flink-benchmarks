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

/**
 * Estimate the effective core clock on a machine with no usable PMU.
 *
 * <p>The measurement box exposes no hardware counters ({@code perf stat} returns the 2^47-1 "not
 * counting" sentinel for cycles/instructions), so proposal section 3.3's "CPU cycles per record"
 * cannot be counted directly.  A dependent chain of single-cycle ALU ops is the standard fallback:
 * the loop cannot be strength-reduced because each iteration's result feeds the next, so
 * iterations per second times the chain length is the clock.  Two chains of different length are
 * measured so the estimate validates itself - if the latency model is wrong the two disagree,
 * which is reported as {@code spread}.
 *
 * <p>Prints one machine-readable line.  Run it pinned to the same core as the benchmark:
 * {@code taskset -c 0 java -cp clockcalib ClockCalib 5}.
 */
public final class ClockCalib {

    private static volatile long sink;

    /** Two dependent ALU ops per iteration: xor then add, both on the loop-carried path. */
    private static long chain2(long iterations, long seed) {
        long x = seed;
        long y = 0x9E3779B97F4A7C15L;
        for (long i = 0; i < iterations; i++) {
            x = x ^ y;
            y = y + x;
        }
        return x ^ y;
    }

    /** Three dependent ALU ops per iteration, same shape, for cross-validation. */
    private static long chain3(long iterations, long seed) {
        long x = seed;
        long y = 0x9E3779B97F4A7C15L;
        for (long i = 0; i < iterations; i++) {
            x = x ^ y;
            y = y + x;
            x = x ^ y;
        }
        return x ^ y;
    }

    /** Effective GHz seen by a dependent chain of {@code chainLength} single-cycle ops. */
    private static double measureGhz(int chainLength, long seconds) {
        long chunk = 20_000_000L;
        long iterations = 0;
        long seed = 1234567L;
        long start = System.nanoTime();
        long deadline = start + seconds * 1_000_000_000L;
        while (System.nanoTime() < deadline) {
            seed = chainLength == 2 ? chain2(chunk, seed) : chain3(chunk, seed);
            iterations += chunk;
        }
        long elapsed = System.nanoTime() - start;
        sink = seed;
        return (double) iterations * chainLength / (double) elapsed;
    }

    public static void main(String[] args) {
        long seconds = args.length > 0 ? Long.parseLong(args[0]) : 5L;
        // Compile both loops before timing either of them.
        measureGhz(2, 1);
        measureGhz(3, 1);
        double ghz2 = measureGhz(2, seconds);
        double ghz3 = measureGhz(3, seconds);
        double mean = (ghz2 + ghz3) / 2.0;
        double spread = Math.abs(ghz2 - ghz3) / mean;
        System.out.printf(
                "CLOCKCALIB ghz_chain2=%.4f ghz_chain3=%.4f ghz=%.4f spread=%.2f%% seconds=%d%n",
                ghz2, ghz3, mean, spread * 100.0, seconds);
    }
}
