/*
 * *
 *  * Copyright 2010 - 2022 JetBrains s.r.o.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * https://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package jetbrains.exodus.tree.ibtree;

import jetbrains.exodus.ByteBufferComparator;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.TreeMap;

public class BTreeTestAddByBatches extends BTreeTestBase {

    private void addKeysByBatches(int entriesToAdd, int batchSize, int checkInterval, Random random) {
        final TreeMap<ByteBuffer, ByteBuffer> expectedMap = new TreeMap<>(ByteBufferComparator.INSTANCE);
        var checker = new ImmutableTreeChecker(expectedMap, random);

        int added = 0;
        int checkIteration = 0;

        int iterations = entriesToAdd / batchSize;
        if (iterations * batchSize < entriesToAdd) {
            iterations++;
        }

        tm = t.getMutableCopy();
        for (int n = 0; n < iterations; n++) {
            var currentBatchSize = Math.min(entriesToAdd - added, batchSize);

            for (int i = 0; i < currentBatchSize; i++) {

            }
        }
    }
}
