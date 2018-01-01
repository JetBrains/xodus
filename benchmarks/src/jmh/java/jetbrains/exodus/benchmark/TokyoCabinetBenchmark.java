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
package jetbrains.exodus.benchmark;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.StringBinding;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Collections;

public class TokyoCabinetBenchmark {

    public static final int KEYS_COUNT = 1000000;
    public static final int WARMUP_ITERATIONS = 10;
    public static final int MEASUREMENT_ITERATIONS = 10;
    public static final int FORKS = 1;

    private static final String PATTERN;
    private static final DecimalFormat FORMAT;

    static {
        PATTERN = "00000000";
        FORMAT = (DecimalFormat) NumberFormat.getIntegerInstance();
        FORMAT.applyPattern(PATTERN);
    }

    private TokyoCabinetBenchmark() {
    }

    public static String[] getSuccessiveStrings(final int count) {
        final String[] result = new String[count];
        for (int i = 0; i < count; i++) {
            result[i] = FORMAT.format(i);
        }
        return result;
    }

    public static String[] getRandomStrings(final int count) {
        final String[] result = getSuccessiveStrings(count);
        shuffleKeys(result);
        return result;
    }

    public static ByteIterable[] getSuccessiveEntries(final int count) {
        final ByteIterable[] result = new ByteIterable[count];
        for (int i = 0; i < count; i++) {
            result[i] = StringBinding.stringToEntry(FORMAT.format(i));
        }
        return result;
    }

    public static ByteIterable[] getRandomEntries(final int count) {
        final ByteIterable[] result = getSuccessiveEntries(count);
        shuffleKeys(result);
        return result;
    }

    public static byte[][] getSuccessiveByteArrays(final int count) {
        final byte[][] result = new byte[count][];
        for (int i = 0; i < count; i++) {
            result[i] = StringBinding.stringToEntry(FORMAT.format(i)).getBytesUnsafe();
        }
        return result;
    }

    public static byte[][] getRandomByteArrays(final int count) {
        final byte[][] result = getSuccessiveByteArrays(count);
        shuffleKeys(result);
        return result;
    }

    public static <T> void shuffleKeys(final T[] keys) {
        Collections.shuffle(Arrays.asList(keys));
    }
}
