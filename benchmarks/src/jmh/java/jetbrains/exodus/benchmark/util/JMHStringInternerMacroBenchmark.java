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

import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
public class JMHStringInternerMacroBenchmark {

    private static final int STRINGS_COUNT = 200000;
    private final StringInterner xdInterner = StringInterner.newInterner(47973);

    private final String[] strings = new String[STRINGS_COUNT];
    private final int[] indices = new int[STRINGS_COUNT];

    @Setup
    public void prepare() {
        for (int i = 0; i < STRINGS_COUNT; ++i) {
            strings[i] = "0000000000" + i;
        }
        Random rng = new Random(3135);
        for (int i = 0; i < STRINGS_COUNT; i++) {
            indices[i] = rng.nextInt(STRINGS_COUNT);
        }
    }

    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 8, time = 1)
    @Fork(value = 5, jvmArgsAppend = "-XX:StringTableSize=43853")
    public int jdkInternDefault() {
        int result = 0;
        for (int index : indices) {
            result += strings[index].intern().charAt(10);
            String again = strings[STRINGS_COUNT - index - 1].intern();
            result += again.charAt(again.length() - 1);
        }
        return result;
    }

    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 8, time = 1)
    @Fork(value = 5, jvmArgsAppend = "-XX:StringTableSize=1024099")
    public int jdkInternSparse() {
        int result = 0;
        for (int index : indices) {
            result += strings[index].intern().charAt(10);
            String again = strings[STRINGS_COUNT - index - 1].intern();
            result += again.charAt(again.length() - 1);
        }
        return result;
    }

    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 8, time = 1)
    @Fork(5)
    public int xdInternDefault() {
        int result = 0;
        for (int index : indices) {
            result += xdInterner.doIntern(strings[index]).charAt(10);
            String again = strings[STRINGS_COUNT - index - 1].intern();
            result += again.charAt(again.length() - 1);
        }
        return result;
    }

    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 8, time = 1)
    @Fork(value = 5, jvmArgsAppend = "-Dexodus.util.stringInternerCacheSize=1024101")
    public int xdInternSparse() {
        int result = 0;
        for (int index : indices) {
            result += xdInterner.doIntern(strings[index]).charAt(10);
            String again = strings[STRINGS_COUNT - index - 1].intern();
            result += again.charAt(again.length() - 1);
        }
        return result;
    }
}
