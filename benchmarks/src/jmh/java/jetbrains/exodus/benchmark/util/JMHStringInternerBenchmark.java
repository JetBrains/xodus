/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.benchmark.util;

import jetbrains.exodus.util.StringInterner;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class JMHStringInternerBenchmark {

    private static final int STRINGS_COUNT = 10000;

    private final String[] strings = new String[STRINGS_COUNT];
    private int i;

    @Setup
    public void prepare() {
        for (int i = 0; i < STRINGS_COUNT; ++i) {
            strings[i] = "0000000000" + i;
        }
    }

    @Setup(Level.Invocation)
    public void prepareOperation() {
        i = (int) (Math.random() * STRINGS_COUNT);
    }

    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(5)
    public String jdkIntern() {
        return strings[i].intern();
    }

    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(5)
    public String xdIntern() {
        return StringInterner.intern(strings[i]);
    }
}
