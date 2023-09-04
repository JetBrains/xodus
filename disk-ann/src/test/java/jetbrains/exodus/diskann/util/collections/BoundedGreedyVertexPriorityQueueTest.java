/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.diskann.util.collections;

import it.unimi.dsi.fastutil.ints.IntDoubleImmutablePair;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;
import java.util.Random;
import java.util.TreeSet;

public class BoundedGreedyVertexPriorityQueueTest {
    @Test
    public void testAddOne() {
        var queue = new BoundedGreedyVertexPriorityQueue(1);
        queue.add(2, 1.0f, true, false);

        Assert.assertEquals(1, queue.size());
        Assert.assertEquals(0, queue.nextNotCheckedVertexIndex());
        Assert.assertEquals(-1, queue.nextNotCheckedVertexIndex());
        Assert.assertEquals(2, queue.vertexIndex(0));
        Assert.assertEquals(1.0, queue.vertexDistance(0), 0.0);
        Assert.assertTrue(queue.isPqDistance(0));
        Assert.assertEquals(1.0, queue.maxDistance(), 0.0);
        var result = new long[10];
        queue.vertexIndices(result, 10);
        Assert.assertArrayEquals(new long[]{2, 0, 0, 0, 0, 0, 0, 0, 0, 0}, result);

        queue.add(3, 2.0f, false, false);

        Assert.assertEquals(1, queue.size());
        Assert.assertEquals(-1, queue.nextNotCheckedVertexIndex());
        Assert.assertEquals(2, queue.vertexIndex(0));
        Assert.assertEquals(1.0, queue.vertexDistance(0), 0.0);
        Assert.assertTrue(queue.isPqDistance(0));
        Assert.assertEquals(1.0, queue.maxDistance(), 0.0);
        result = new long[1];
        queue.vertexIndices(result, 1);
        Assert.assertArrayEquals(new long[]{2}, result);

        queue.add(4, 0.5f, false, false);

        Assert.assertEquals(1, queue.size());
        Assert.assertEquals(0, queue.nextNotCheckedVertexIndex());
        Assert.assertEquals(4, queue.vertexIndex(0));
        Assert.assertEquals(0.5, queue.vertexDistance(0), 0.0);
        Assert.assertFalse(queue.isPqDistance(0));
        Assert.assertEquals(0.5, queue.maxDistance(), 0.0);
        result = new long[0];
        queue.vertexIndices(result, 0);
        Assert.assertArrayEquals(new long[]{}, result);
        result = new long[2];
        queue.vertexIndices(result, 2);

        Assert.assertArrayEquals(new long[]{4, 0}, result);
    }

    @Test
    public void testAddTwo() {
        var queue = new BoundedGreedyVertexPriorityQueue(2);

        Assert.assertEquals(0, queue.size());

        queue.add(1, 1.3f, true, false);
        Assert.assertEquals(1, queue.size());

        queue.add(2, 1.0f, false, false);

        Assert.assertEquals(2, queue.size());

        Assert.assertEquals(0, queue.nextNotCheckedVertexIndex());
        Assert.assertEquals(1, queue.nextNotCheckedVertexIndex());
        Assert.assertEquals(-1, queue.nextNotCheckedVertexIndex());

        Assert.assertEquals(2, queue.vertexIndex(0));
        Assert.assertEquals(1, queue.vertexIndex(1));

        Assert.assertEquals(1.0f, queue.vertexDistance(0), 0.0);
        Assert.assertEquals(1.3f, queue.vertexDistance(1), 0.0);

        Assert.assertFalse(queue.isPqDistance(0));
        Assert.assertTrue(queue.isPqDistance(1));

        Assert.assertEquals(1.3f, queue.maxDistance(), 0.0);
        var result = new long[10];
        queue.vertexIndices(result, 10);
        Assert.assertArrayEquals(new long[]{2, 1, 0, 0, 0, 0, 0, 0, 0, 0}, result);

        queue.add(3, 3, false, false);

        Assert.assertEquals(2, queue.size());
        Assert.assertEquals(-1, queue.nextNotCheckedVertexIndex());

        Assert.assertEquals(2, queue.vertexIndex(0));
        Assert.assertEquals(1, queue.vertexIndex(1));

        Assert.assertEquals(1.0f, queue.vertexDistance(0), 0.0);
        Assert.assertEquals(1.3f, queue.vertexDistance(1), 0.0);

        Assert.assertEquals(1.3, queue.maxDistance(), 0.01);
        result = new long[1];
        queue.vertexIndices(result, 1);

        Assert.assertArrayEquals(new long[]{2}, result);
        result = new long[2];
        queue.vertexIndices(result, 2);

        Assert.assertArrayEquals(new long[]{2, 1}, result);

        result = new long[10];
        queue.vertexIndices(result, 10);

        Assert.assertArrayEquals(new long[]{2, 1, 0, 0, 0, 0, 0, 0, 0, 0}, result);

        queue.add(3, 0.5f, false, false);

        Assert.assertEquals(2, queue.size());
        Assert.assertEquals(0, queue.nextNotCheckedVertexIndex());
        Assert.assertEquals(-1, queue.nextNotCheckedVertexIndex());

        Assert.assertEquals(3, queue.vertexIndex(0));
        Assert.assertEquals(2, queue.vertexIndex(1));

        Assert.assertEquals(0.5f, queue.vertexDistance(0), 0.01);
        Assert.assertEquals(1.0f, queue.vertexDistance(1), 0.01);

        Assert.assertFalse(queue.isPqDistance(0));
        Assert.assertFalse(queue.isPqDistance(1));

        result = new long[10];
        queue.vertexIndices(result, 10);

        Assert.assertArrayEquals(new long[]{3, 2, 0, 0, 0, 0, 0, 0, 0, 0}, result);
        Assert.assertEquals(1.0, queue.maxDistance(), 0.0);

        queue.add(4, 0.75f, true, false);
        Assert.assertEquals(4, queue.vertexIndex(1));

        queue.resortVertex(1, 0.25f);

        Assert.assertEquals(4, queue.vertexIndex(0));
        Assert.assertEquals(0.25, queue.vertexDistance(0), 0.0);

        Assert.assertEquals(3, queue.vertexIndex(1));
        Assert.assertEquals(0.5, queue.vertexDistance(1), 0.0);

        Assert.assertEquals(2, queue.size());
        Assert.assertEquals(0, queue.nextNotCheckedVertexIndex());
        Assert.assertEquals(-1, queue.nextNotCheckedVertexIndex());

        result = new long[10];
        queue.vertexIndices(result, 10);

        Assert.assertArrayEquals(new long[]{4, 3, 0, 0, 0, 0, 0, 0, 0, 0}, result);
        Assert.assertEquals(0.5, queue.maxDistance(), 0.0);
    }

    @Test
    public void testAddThree() {
        var queue = new BoundedGreedyVertexPriorityQueue(3);
        Assert.assertEquals(0, queue.size());

        queue.add(1, 1.3f, true, false);
        Assert.assertEquals(1, queue.size());

        queue.add(2, 1.0f, false, false);
        Assert.assertEquals(2, queue.size());

        queue.add(3, 0.5f, true, false);
        Assert.assertEquals(3, queue.size());


        Assert.assertEquals(0, queue.nextNotCheckedVertexIndex());
        Assert.assertEquals(1, queue.nextNotCheckedVertexIndex());
        Assert.assertEquals(2, queue.nextNotCheckedVertexIndex());
        Assert.assertEquals(-1, queue.nextNotCheckedVertexIndex());

        Assert.assertEquals(3, queue.vertexIndex(0));
        Assert.assertEquals(2, queue.vertexIndex(1));
        Assert.assertEquals(1, queue.vertexIndex(2));

        queue.resortVertex(1, 0.9f);

        Assert.assertEquals(3, queue.vertexIndex(0));
        Assert.assertEquals(2, queue.vertexIndex(1));
        Assert.assertEquals(1, queue.vertexIndex(2));

        Assert.assertEquals(1, queue.nextNotCheckedVertexIndex());
        Assert.assertEquals(-1, queue.nextNotCheckedVertexIndex());

        queue.resortVertex(1, 1.1f);

        Assert.assertEquals(3, queue.vertexIndex(0));
        Assert.assertEquals(2, queue.vertexIndex(1));
        Assert.assertEquals(1, queue.vertexIndex(2));

        Assert.assertEquals(1, queue.nextNotCheckedVertexIndex());
        Assert.assertEquals(-1, queue.nextNotCheckedVertexIndex());

        queue.resortVertex(2, 0.75f);

        Assert.assertEquals(3, queue.vertexIndex(0));
        Assert.assertEquals(1, queue.vertexIndex(1));
        Assert.assertEquals(2, queue.vertexIndex(2));

        Assert.assertEquals(1, queue.nextNotCheckedVertexIndex());
        Assert.assertEquals(-1, queue.nextNotCheckedVertexIndex());
    }

    @Test
    public void rndTest() {
        var seed = System.nanoTime();
        var rnd = new Random(seed);
        System.out.println("rndTest seed = " + seed);

        var itemsCount = 200;
        var queue = new BoundedGreedyVertexPriorityQueue(itemsCount);
        var treeSet = new TreeSet<>(Comparator.comparing(IntDoubleImmutablePair::rightDouble));

        for (int i = 0; i < 400; i++) {
            var vertexIndex = rnd.nextInt(Integer.MAX_VALUE);
            var distance = 10 * rnd.nextFloat();
            var isPqDistance = rnd.nextBoolean();

            if (treeSet.add(new IntDoubleImmutablePair(vertexIndex, distance))) {
                queue.add(vertexIndex, distance, isPqDistance, false);
            }

            if (treeSet.size() > itemsCount) {
                treeSet.pollLast();
            }

            Assert.assertEquals(treeSet.size(), queue.size());
            Assert.assertEquals(treeSet.last().rightDouble(), queue.maxDistance(), 0.0);

            var treeSetIterator = treeSet.iterator();
            for (int j = 0; j < treeSet.size(); j++) {
                var treeSetElement = treeSetIterator.next();
                Assert.assertEquals(treeSetElement.leftInt(), queue.vertexIndex(j));
                Assert.assertEquals(treeSetElement.rightDouble(), queue.vertexDistance(j), 0.0);
            }
        }

        var indexes = new int[itemsCount];
        for (int i = 0; i < itemsCount; i++) {
            indexes[i] = i;
        }
        ArrayUtils.shuffle(indexes, rnd);

        for (var index : indexes) {
            var distance = queue.vertexDistance(index);
            var vertexIndex = queue.vertexIndex(index);
            var newDistance = 10 * rnd.nextFloat();

            queue.resortVertex(index, newDistance);

            Assert.assertTrue(treeSet.remove(new IntDoubleImmutablePair(vertexIndex, distance)));
            Assert.assertTrue(treeSet.add(new IntDoubleImmutablePair(vertexIndex, newDistance)));
        }

        for (int i = 0; i < 200; i++) {
            var treeSetElement = treeSet.pollFirst();
            Assert.assertNotNull(treeSetElement);

            Assert.assertEquals(treeSetElement.leftInt(), queue.vertexIndex(i));
            Assert.assertEquals(treeSetElement.rightDouble(), queue.vertexDistance(i), 0.0);
        }
    }
}
