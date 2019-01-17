/**
 * Copyright 2010 - 2019 JetBrains s.r.o.
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

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.StringBinding;
import kotlin.text.Charsets;
import org.openjdk.jmh.annotations.*;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class JMHStringBindingTest {

    private static final int STRINGS_COUNT = 10000;

    private final String[] strings = new String[STRINGS_COUNT];
    private final byte[][] bytes = new byte[STRINGS_COUNT][];
    private final ByteIterable[] byteIterables = new ByteIterable[STRINGS_COUNT];

    @Setup
    public void prepare() {
        for (int i = 0; i < STRINGS_COUNT; ++i) {
            strings[i] = UUID.randomUUID().toString();
            bytes[i] = strings[i].getBytes(Charsets.UTF_8);
            byteIterables[i] = StringBinding.stringToEntry(strings[i]);
        }
    }

    @Benchmark
    @Warmup(iterations = 2, time = 1)
    @Measurement(iterations = 3, time = 1)
    @Fork(4)
    public String[] byteArray2string() {
        String[] result = new String[STRINGS_COUNT];
        int i = 0;
        for (byte[] array : bytes) {
            result[i++] = new String(array, Charsets.UTF_8);
        }
        return result;
    }

    @Benchmark
    @Warmup(iterations = 2, time = 1)
    @Measurement(iterations = 3, time = 1)
    @Fork(4)
    public byte[][] byteArray2byteArray() {
        byte[][] result = new byte[STRINGS_COUNT][];
        int i = 0;
        for (String string : strings) {
            result[i++] = string.getBytes(Charsets.UTF_8);
        }
        return result;
    }

    @Benchmark
    @Warmup(iterations = 2, time = 1)
    @Measurement(iterations = 3, time = 1)
    @Fork(4)
    public String[] stringBinding2string() {
        String[] result = new String[STRINGS_COUNT];
        int i = 0;
        for (ByteIterable array : byteIterables) {
            result[i++] = StringBinding.entryToString(array);
        }
        return result;
    }

    @Benchmark
    @Warmup(iterations = 2, time = 1)
    @Measurement(iterations = 3, time = 1)
    @Fork(4)
    public ByteIterable[] stringBinding2byteArray() {
        ByteIterable[] result = new ByteIterable[STRINGS_COUNT];
        int i = 0;
        for (String str : strings) {
            result[i++] = StringBinding.stringToEntry(str);
        }
        return result;
    }

}

