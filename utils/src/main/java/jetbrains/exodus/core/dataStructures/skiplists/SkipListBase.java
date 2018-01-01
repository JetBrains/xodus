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
package jetbrains.exodus.core.dataStructures.skiplists;

import jetbrains.exodus.util.Random;

class SkipListBase {

    private static final int[] LEVEL_FACTORS = {7, 5, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3};

    private final Random rnd;
    private int seed;
    protected int size;

    SkipListBase() {
        rnd = new Random();
        seed = rnd.nextInt() & 0x7fffffff;
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * @return level of a node using quasi-random heuristics in consideration that
     * certain divisors are distributed quite uniformly amongst successive natural numbers.
     */
    protected int generateQuasiRandomLevel() {
        int level = 1;
        int seed = this.seed + 1;
        if ((seed & 0x3ff) == 0) {
            seed = rnd.nextInt();
        }
        seed &= 0x7fffffff;
        this.seed = seed;
        for (; ; ) {
            final int factor = LEVEL_FACTORS[level - 1];
            if (seed % factor != 1) break;
            seed /= factor;
            ++level;
        }
        return level;
    }
}
