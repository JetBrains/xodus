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
import jetbrains.exodus.tree.ITreeMutable;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.TreeMap;

public class BTreeAddTest extends BTreeTestBase {
    @Test
    public void singleAddGet() {
        final long seed = System.nanoTime();
        System.out.println("singleAddGet seed : " + seed);

        var tm = createMutableTree(false, 2);
        var random = new Random(seed);

        var entriesCount = 1;

        addAndCheckEntries(tm, random, entriesCount);
    }

    @Test
    public void testAdd4Entries() {
        final long seed = System.nanoTime();
        System.out.println("testAdd4Entries seed : " + seed);

        var tm = createMutableTree(false, 2);
        var random = new Random(seed);

        var entriesCount = 4;

        addAndCheckEntries(tm, random, entriesCount);
    }

    @Test
    public void testAdd64Entries() {
        final long seed = System.nanoTime();
        System.out.println("testAdd64Entries seed : " + seed);

        var tm = createMutableTree(false, 2);
        var random = new Random(seed);

        var entriesCount = 64;

        addAndCheckEntries(tm, random, entriesCount);
    }

    @Test
    public void testAdd1KEntries() {
        final long seed = System.nanoTime();
        System.out.println("testAdd1KEntries seed : " + seed);

        var tm = createMutableTree(false, 2);
        var random = new Random(seed);

        var entriesCount = 1024;

        addAndCheckEntries(tm, random, entriesCount);
    }

    @Test
    public void testAdd32KEntries() {
        final long seed = System.nanoTime();
        System.out.println("testAdd32KEntries seed : " + seed);

        var tm = createMutableTree(false, 2);
        var random = new Random(seed);

        var entriesCount = 32 * 1024;

        addAndCheckEntries(tm, random, entriesCount);
    }

    @Test
    public void testAdd64KEntries() {
        final long seed = System.nanoTime();
        System.out.println("testAdd64KEntries seed : " + seed);

        var tm = createMutableTree(false, 2);
        var random = new Random(seed);

        var entriesCount = 64 * 1024;

        addAndCheckEntries(tm, random, entriesCount);
    }

    private void addAndCheckEntries(ITreeMutable tm, Random random, int entriesCount) {
        final TreeMap<ByteBuffer, ByteBuffer> expectedMap = new TreeMap<>(ByteBufferComparator.INSTANCE);
        for (int i = 0; i < entriesCount; i++) {

            var keySize = random.nextInt(1, 16);
            var keyArray = new byte[keySize];
            random.nextBytes(keyArray);
            var key = ByteBuffer.wrap(keyArray);

            var valueArray = new byte[32];
            random.nextBytes(valueArray);
            var value = ByteBuffer.wrap(valueArray);

            if (expectedMap.containsKey(key)) {
                var added = tm.add(key, value);
                Assert.assertFalse(added);
            } else {
                expectedMap.put(key, value);
                var added = tm.add(key, value);
                Assert.assertTrue(added);
            }
        }

        var keys = new ArrayList<>(expectedMap.keySet());
        Collections.shuffle(keys);

        var singleByte = ByteBuffer.allocate(1);
        for (var key : keys) {
            var added = tm.add(key, singleByte);
            Assert.assertFalse(added);
        }

        checkAndSaveTree(false, new ImmutableTreeChecker(expectedMap, random));
    }
}
