/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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
package jetbrains.exodus.benchmark.env;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.util.ByteIterableUtil;
import jetbrains.exodus.util.ByteIterableUtil2;

import java.text.DecimalFormat;
import java.text.NumberFormat;

public class ComparePerformance {
    private static final int SIZE = 5000 * 2;
    public static final int ITERATIONS = 8;
    private static final ByteIterable[] KEYS = new ByteIterable[SIZE];

    private ComparePerformance() {
    }

    public static void main(String[] args) {
        final DecimalFormat format = (DecimalFormat) NumberFormat.getIntegerInstance();
        final String megaString = "00000000000000000000000000000000000000000000000000000000000000000000000000000000";
        final int max = megaString.length();
        for (int i = 0; i < max; i++) {
            System.out.printf("[%02d]-----------------------------------\n", i);
            fillKeys(format, megaString.substring(0, i));
            test();
            System.out.println();
        }
    }

    private static void fillKeys(DecimalFormat format, String pattern) {
        format.applyPattern(pattern);
        for (int i = 0; i < SIZE; i++) {
            KEYS[i] = StringBinding.stringToEntry(format.format(i));
        }
    }

    private static void test() {
        for (int i = 0; i < ITERATIONS; i++) {
            long time = System.currentTimeMillis();
            cmp();
            System.out.printf("%4d ", System.currentTimeMillis() - time);
        }
        System.out.println();
        for (int i = 0; i < ITERATIONS; i++) {
            long time = System.currentTimeMillis();
            cmp2();
            System.out.printf("%4d ", System.currentTimeMillis() - time);
        }
        System.out.println();
        for (int i = 0; i < ITERATIONS; i++) {
            long time = System.currentTimeMillis();
            cmp3();
            System.out.printf("%4d ", System.currentTimeMillis() - time);
        }
        System.out.println();
        for (int i = 0; i < ITERATIONS; i++) {
            long time = System.currentTimeMillis();
            cmp4();
            System.out.printf("%4d ", System.currentTimeMillis() - time);
        }
    }

    private static void cmp() {
        for (int i = 0; i < SIZE; i++) {
            for (int j = 0; j < SIZE; j++) {
                ByteIterableUtil.compare(KEYS[i], KEYS[j]);
            }
        }
    }

    private static void cmp2() {
        for (int i = 0; i < SIZE; i++) {
            for (int j = 0; j < SIZE; j++) {
                ByteIterableUtil2.compare2(KEYS[i], KEYS[j]);
            }
        }
    }

    private static void cmp3() {
        for (int i = 0; i < SIZE; i++) {
            for (int j = 0; j < SIZE; j++) {
                ByteIterableUtil2.compare3(KEYS[i], KEYS[j]);
            }
        }
    }

    private static void cmp4() {
        for (int i = 0; i < SIZE; i++) {
            for (int j = 0; j < SIZE; j++) {
                ByteIterableUtil2.compare4(KEYS[i], KEYS[j]);
            }
        }
    }
}
