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
import jetbrains.exodus.ByteBufferByteIterable;
import jetbrains.exodus.ByteBufferComparator;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.tree.ITreeMutable;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.TreeMap;

public class BasicBTreeTest extends BTreeTestBase {
    @Test
    public void checkEmptyTree() {
        var tm = createMutableTree(false, 1);
        checkEmptyTree(tm);

        long address = saveTree();

        checkEmptyTree(tm);

        var t = openTree(address, false);

        checkEmptyTree(t);

        reopen();

        t = openTree(address, false);
        checkEmptyTree(t);
    }

    @Test
    public void singlePutGet() {
        var tm = createMutableTree(false, 1);
        tm.put(key(1), value("v1"));

        checkTree(false, t -> {
            Assert.assertEquals(1, t.getSize());
            Assert.assertFalse(t.isEmpty());

            Assert.assertEquals(value("v1"), t.get(key(1)));
            Assert.assertTrue(t.hasKey(key(1)));
            Assert.assertTrue(t.hasPair(key(1), value("v1")));

            Assert.assertNull(t.get(key(2)));
            Assert.assertFalse(t.hasPair(key(1), value("v2")));
            Assert.assertFalse(t.hasPair(key(2), value("v1")));
            Assert.assertFalse(t.hasPair(key(2), value("v2")));
            Assert.assertFalse(t.hasKey(key(2)));

            try (var cursor = t.openCursor()) {
                Assert.assertTrue(cursor.getNext());

                Assert.assertEquals(key(1), cursor.getKey());
                Assert.assertEquals(value("v1"), cursor.getValue());

                Assert.assertFalse(cursor.getNext());

                Assert.assertEquals(ByteIterable.EMPTY, cursor.getKey());
                Assert.assertEquals(ByteIterable.EMPTY, cursor.getValue());
            }

            try (var cursor = t.openCursor()) {
                Assert.assertTrue(cursor.getPrev());

                Assert.assertEquals(key(1), cursor.getKey());
                Assert.assertEquals(value("v1"), cursor.getValue());

                Assert.assertFalse(cursor.getNext());

                Assert.assertEquals(ByteIterable.EMPTY, cursor.getKey());
                Assert.assertEquals(ByteIterable.EMPTY, cursor.getValue());
            }
        });
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
        final long seed = 8444372607505L;// System.nanoTime();
        System.out.println("testInsert1KEntries seed : " + seed);

        var tm = createMutableTree(false, 2);
        var random = new Random(seed);

        var entriesCount = 1024;

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

        var keys = new ArrayList<>(expectedMap.keySet());
        Collections.shuffle(keys, random);

        checkTree(false, t -> {
            for (var key : keys) {
                var value = t.get(new ByteBufferByteIterable(key));
                var expectedValue = expectedMap.get(key);

                Assert.assertEquals(new ByteBufferByteIterable(expectedValue), value);
            }
        });
    }
}
