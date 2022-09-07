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

import jetbrains.exodus.ByteBufferByteIterable;
import jetbrains.exodus.env.Cursor;
import jetbrains.exodus.tree.ITree;
import org.junit.Assert;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Consumer;

public class ImmutableTreeChecker implements Consumer<ITree> {
    final TreeMap<ByteBuffer, ByteBuffer> expectedMap;
    final Random random;

    public ImmutableTreeChecker(TreeMap<ByteBuffer, ByteBuffer> expectedMap, Random random) {
        this.expectedMap = expectedMap;
        this.random = random;

    }

    @Override
    public void accept(ITree tree) {
        Assert.assertEquals(expectedMap.size(), tree.getSize());

        var keys = new ArrayList<ByteBuffer>(expectedMap.size() + expectedMap.size() / 10 + 1);
        keys.addAll(expectedMap.keySet());

        for (int i = 0; i < expectedMap.size() / 10; i++) {
            var keySize = random.nextInt(1, 64);
            var keyArray = new byte[keySize];

            random.nextBytes(keyArray);

            var key = ByteBuffer.wrap(keyArray);
            if (!expectedMap.containsKey(key)) {
                keys.add(key);
            }
        }

        Collections.shuffle(keys, random);

        for (var key : keys) {
            var value = tree.get(new ByteBufferByteIterable(key));
            var expectedValue = expectedMap.get(key);

            if (expectedValue != null) {
                Assert.assertEquals(new ByteBufferByteIterable(expectedValue), value);
            } else {
                Assert.assertNull(value);
            }
        }

        try (var cursor = tree.openCursor()) {
            checkForwardCursor(expectedMap.entrySet().iterator(), cursor);
        }

        try (var cursor = tree.openCursor()) {
            checkBackwardCursor(expectedMap.descendingMap().entrySet().iterator(), cursor);
        }

        var step = Math.max(keys.size() / 10, 1);
        for (int i = 0; i < keys.size(); i += step) {
            var key = keys.get(i);

            var expectedEntry = expectedMap.get(key);
            if (expectedEntry != null) {
                checkExistingKey(tree, key);
            } else {
                checkKeyWhichMayNotExist(tree, key);
            }

            var modifiedKeyArray = new byte[key.limit()];
            key.get(0, modifiedKeyArray);
            modifiedKeyArray[0]++;
            var modifiedKey = ByteBuffer.wrap(modifiedKeyArray);

            if (i == 10167) {
                System.out.println();
            }

            checkKeyWhichMayNotExist(tree, modifiedKey);
        }
    }

    private void checkKeyWhichMayNotExist(ITree tree, ByteBuffer modifiedKey) {
        try (var cursor = tree.openCursor()) {
            var value = cursor.getSearchKeyRange(modifiedKey);
            var expectedEntry = expectedMap.ceilingEntry(modifiedKey);

            if (expectedEntry != null) {
                Assert.assertEquals(expectedEntry.getValue(), value);

                Assert.assertEquals(expectedEntry.getValue(), cursor.getValueBuffer());
                Assert.assertEquals(expectedEntry.getKey(), cursor.getKeyBuffer());

                var tailMap = expectedMap.tailMap(expectedEntry.getKey(), false);
                var forwardIterator = tailMap.entrySet().iterator();

                checkForwardCursor(forwardIterator, cursor);
            } else {
                Assert.assertNull(value);
                Assert.assertFalse(cursor.getNext());
            }
        }

        try (var cursor = tree.openCursor()) {
            Iterator<Map.Entry<ByteBuffer, ByteBuffer>> backwardIterator;
            var value = cursor.getSearchKeyRange(modifiedKey);

            if (value != null) {
                var expectedEntry = expectedMap.ceilingEntry(modifiedKey);

                if (expectedEntry != null) {
                    backwardIterator =
                            expectedMap.headMap(expectedEntry.getKey(), false).
                                    descendingMap().entrySet().iterator();
                } else {
                    backwardIterator = expectedMap.descendingMap().entrySet().iterator();
                    if (backwardIterator.hasNext()) {
                        backwardIterator.next();
                    } else {
                        Assert.assertFalse(cursor.getPrev());
                    }
                }

                checkBackwardCursor(backwardIterator, cursor);
            } else {
                Assert.assertNull(cursor.getKeyBuffer());
                Assert.assertNull(cursor.getValueBuffer());
            }
        }
    }

    private void checkExistingKey(ITree tree, ByteBuffer key) {
        var forwardIterator =
                expectedMap.tailMap(key, false).entrySet().iterator();
        try (var cursor = tree.openCursor()) {
            var value = cursor.getSearchKey(key);
            Assert.assertNotNull(value);

            var expectedValue = expectedMap.get(key);
            Assert.assertEquals(value, expectedValue);

            Assert.assertEquals(key, cursor.getKeyBuffer());
            Assert.assertEquals(expectedValue, cursor.getValueBuffer());

            checkForwardCursor(forwardIterator, cursor);
        }

        var backwardIterator = expectedMap.headMap(key, false).descendingMap()
                .entrySet().iterator();
        try (var cursor = tree.openCursor()) {
            var value = cursor.getSearchKey(new ByteBufferByteIterable(key));
            Assert.assertNotNull(value);

            var expectedValue = expectedMap.get(key);
            Assert.assertEquals(value.getByteBuffer(), expectedValue);

            Assert.assertEquals(key, cursor.getKeyBuffer());
            Assert.assertEquals(expectedValue, cursor.getValueBuffer());

            checkBackwardCursor(backwardIterator, cursor);
        }
    }

    private void checkForwardCursor(Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator, final Cursor cursor) {
        while (iterator.hasNext()) {
            Assert.assertTrue(cursor.getNext());

            var entry = iterator.next();
            var expectedKey = entry.getKey();
            var key = cursor.getKeyBuffer();

            Assert.assertEquals(expectedKey, key);

            var value = entry.getValue();
            var expectedValue = cursor.getValueBuffer();

            Assert.assertEquals(value, expectedValue);

            Assert.assertEquals(1, cursor.count());
        }

        Assert.assertFalse(cursor.getNext());
        Assert.assertEquals(0, cursor.count());
    }

    private void checkBackwardCursor(Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator, final Cursor cursor) {
        while (iterator.hasNext()) {
            Assert.assertTrue(cursor.getPrev());

            var entry = iterator.next();
            Assert.assertEquals(entry.getKey(), cursor.getKey().getByteBuffer());
            Assert.assertEquals(entry.getValue(), cursor.getValue().getByteBuffer());

            Assert.assertEquals(1, cursor.count());
        }

        Assert.assertFalse(cursor.getPrev());
        Assert.assertEquals(0, cursor.count());
    }
}
