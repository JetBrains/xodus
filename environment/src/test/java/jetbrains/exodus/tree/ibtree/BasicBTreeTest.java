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

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteBufferComparator;
import jetbrains.exodus.tree.ITreeMutable;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;

public class BasicBTreeTest extends BTreeTestBase {
    @Test
    public void checkEmptyTree() {
        final long seed = System.nanoTime();
        System.out.println("checkEmptyTree seed : " + seed);

        var tm = createMutableTree(false, 2);
        var random = new Random(seed);

        var entriesCount = 0;

        insertAndCheckEntries(tm, random, entriesCount);
    }

    @Test
    public void singlePutGet() {
        final long seed = System.nanoTime();
        System.out.println("singlePutGet seed : " + seed);

        var tm = createMutableTree(false, 2);
        var random = new Random(seed);

        var entriesCount = 1;

        insertAndCheckEntries(tm, random, entriesCount);
    }

    @Test
    public void testInsert4Entries() {
        final long seed = System.nanoTime();
        System.out.println("testInsert4Entries seed : " + seed);

        var tm = createMutableTree(false, 2);
        var random = new Random(seed);

        var entriesCount = 4;

        insertAndCheckEntries(tm, random, entriesCount);
    }

    @Test
    public void testInsert64Entries() {
        final long seed = System.nanoTime();
        System.out.println("testInsert64Entries seed : " + seed);

        var tm = createMutableTree(false, 2);
        var random = new Random(seed);

        var entriesCount = 64;

        insertAndCheckEntries(tm, random, entriesCount);
    }

    @Test
    public void testInsert1KEntries() {
        final long seed = System.nanoTime();
        System.out.println("testInsert1KEntries seed : " + seed);

        var tm = createMutableTree(false, 2);
        var random = new Random(seed);

        var entriesCount = 1024;

        insertAndCheckEntries(tm, random, entriesCount);
    }

    @Test
    public void testInsert32KEntries() {
        final long seed = System.nanoTime();
        System.out.println("testInsert32KEntries seed : " + seed);

        var tm = createMutableTree(false, 2);
        var random = new Random(seed);

        var entriesCount = 32 * 1024;

        insertAndCheckEntries(tm, random, entriesCount);
    }

    @Test
    public void testInsert64KEntries() {
        final long seed = System.nanoTime();
        System.out.println("testInsert64KEntries seed : " + seed);

        var tm = createMutableTree(false, 2);
        var random = new Random(seed);

        var entriesCount = 64 * 1024;

        insertAndCheckEntries(tm, random, entriesCount);
    }

    @Test
    public void testInsert256KEntries() {
        final long seed = System.nanoTime();
        System.out.println("testInsert256KEntries seed : " + seed);

        var tm = createMutableTree(false, 2);
        var random = new Random(seed);

        var entriesCount = 256 * 1024;

        insertAndCheckEntries(tm, random, entriesCount);
    }


    private void insertAndCheckEntries(ITreeMutable tm, Random random, int entriesCount) {
        final TreeMap<ByteBuffer, ByteBuffer> expectedMap = new TreeMap<>(ByteBufferComparator.INSTANCE);
        for (int i = 0; i < entriesCount; i++) {
            var keySize = random.nextInt(1, 16);
            var key = new byte[keySize];
            random.nextBytes(key);

            var value = new byte[32];
            random.nextBytes(value);

            expectedMap.put(ByteBuffer.wrap(key), ByteBuffer.wrap(value));
            tm.put(new ArrayByteIterable(key), new ArrayByteIterable(value));
        }

        checkAndSaveTree(false, new ImmutableTreeChecker(expectedMap, random));
    }
}
