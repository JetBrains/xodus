package jetbrains.exodus.diskann.collections;

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
        queue.add(2, 1.0, true);

        Assert.assertEquals(1, queue.size());
        Assert.assertEquals(0, queue.nextNotCheckedVertexIndex());
        Assert.assertEquals(-1, queue.nextNotCheckedVertexIndex());
        Assert.assertEquals(2, queue.vertexIndex(0));
        Assert.assertEquals(1.0, queue.vertexDistance(0), 0.0);
        Assert.assertTrue(queue.isPqDistance(0));
        Assert.assertEquals(1.0, queue.maxDistance(), 0.0);
        Assert.assertArrayEquals(new int[]{2}, queue.vertexIndices(10));

        queue.add(3, 2.0, false);

        Assert.assertEquals(1, queue.size());
        Assert.assertEquals(-1, queue.nextNotCheckedVertexIndex());
        Assert.assertEquals(2, queue.vertexIndex(0));
        Assert.assertEquals(1.0, queue.vertexDistance(0), 0.0);
        Assert.assertTrue(queue.isPqDistance(0));
        Assert.assertEquals(1.0, queue.maxDistance(), 0.0);
        Assert.assertArrayEquals(new int[]{2}, queue.vertexIndices(1));

        queue.add(4, 0.5, false);

        Assert.assertEquals(1, queue.size());
        Assert.assertEquals(0, queue.nextNotCheckedVertexIndex());
        Assert.assertEquals(4, queue.vertexIndex(0));
        Assert.assertEquals(0.5, queue.vertexDistance(0), 0.0);
        Assert.assertFalse(queue.isPqDistance(0));
        Assert.assertEquals(0.5, queue.maxDistance(), 0.0);
        Assert.assertArrayEquals(new int[]{}, queue.vertexIndices(0));
        Assert.assertArrayEquals(new int[]{4}, queue.vertexIndices(2));
    }

    @Test
    public void testAddTwo() {
        var queue = new BoundedGreedyVertexPriorityQueue(2);

        Assert.assertEquals(0, queue.size());

        queue.add(1, 1.3, true);
        Assert.assertEquals(1, queue.size());

        queue.add(2, 1.0, false);

        Assert.assertEquals(2, queue.size());

        Assert.assertEquals(0, queue.nextNotCheckedVertexIndex());
        Assert.assertEquals(1, queue.nextNotCheckedVertexIndex());
        Assert.assertEquals(-1, queue.nextNotCheckedVertexIndex());

        Assert.assertEquals(2, queue.vertexIndex(0));
        Assert.assertEquals(1, queue.vertexIndex(1));

        Assert.assertEquals(1.0, queue.vertexDistance(0), 0.0);
        Assert.assertEquals(1.3, queue.vertexDistance(1), 0.0);

        Assert.assertFalse(queue.isPqDistance(0));
        Assert.assertTrue(queue.isPqDistance(1));

        Assert.assertEquals(1.3, queue.maxDistance(), 0.0);
        Assert.assertArrayEquals(new int[]{2, 1}, queue.vertexIndices(10));


        queue.add(3, 3, false);

        Assert.assertEquals(2, queue.size());
        Assert.assertEquals(-1, queue.nextNotCheckedVertexIndex());

        Assert.assertEquals(2, queue.vertexIndex(0));
        Assert.assertEquals(1, queue.vertexIndex(1));

        Assert.assertEquals(1.0, queue.vertexDistance(0), 0.0);
        Assert.assertEquals(1.3, queue.vertexDistance(1), 0.0);

        Assert.assertEquals(1.3, queue.maxDistance(), 0.0);
        Assert.assertArrayEquals(new int[]{2}, queue.vertexIndices(1));
        Assert.assertArrayEquals(new int[]{2, 1}, queue.vertexIndices(2));
        Assert.assertArrayEquals(new int[]{2, 1}, queue.vertexIndices(10));


        queue.add(3, 0.5, false);

        Assert.assertEquals(2, queue.size());
        Assert.assertEquals(0, queue.nextNotCheckedVertexIndex());
        Assert.assertEquals(-1, queue.nextNotCheckedVertexIndex());

        Assert.assertEquals(3, queue.vertexIndex(0));
        Assert.assertEquals(2, queue.vertexIndex(1));

        Assert.assertEquals(0.5, queue.vertexDistance(0), 0.0);
        Assert.assertEquals(1.0, queue.vertexDistance(1), 0.0);

        Assert.assertFalse(queue.isPqDistance(0));
        Assert.assertFalse(queue.isPqDistance(1));

        Assert.assertArrayEquals(new int[]{3, 2}, queue.vertexIndices(10));
        Assert.assertEquals(1.0, queue.maxDistance(), 0.0);

        queue.add(4, 0.75, true);
        Assert.assertEquals(4, queue.vertexIndex(1));

        queue.resortVertex(1, 0.25);

        Assert.assertEquals(4, queue.vertexIndex(0));
        Assert.assertEquals(0.25, queue.vertexDistance(0), 0.0);

        Assert.assertEquals(3, queue.vertexIndex(1));
        Assert.assertEquals(0.5, queue.vertexDistance(1), 0.0);

        Assert.assertEquals(2, queue.size());
        Assert.assertEquals(0, queue.nextNotCheckedVertexIndex());
        Assert.assertEquals(-1, queue.nextNotCheckedVertexIndex());

        Assert.assertArrayEquals(new int[]{4, 3}, queue.vertexIndices(10));
        Assert.assertEquals(0.5, queue.maxDistance(), 0.0);
    }

    @Test
    public void testAddThree() {
        var queue = new BoundedGreedyVertexPriorityQueue(3);
        Assert.assertEquals(0, queue.size());

        queue.add(1, 1.3, true);
        Assert.assertEquals(1, queue.size());

        queue.add(2, 1.0, false);
        Assert.assertEquals(2, queue.size());

        queue.add(3, 0.5, true);
        Assert.assertEquals(3, queue.size());


        Assert.assertEquals(0, queue.nextNotCheckedVertexIndex());
        Assert.assertEquals(1, queue.nextNotCheckedVertexIndex());
        Assert.assertEquals(2, queue.nextNotCheckedVertexIndex());
        Assert.assertEquals(-1, queue.nextNotCheckedVertexIndex());

        Assert.assertEquals(3, queue.vertexIndex(0));
        Assert.assertEquals(2, queue.vertexIndex(1));
        Assert.assertEquals(1, queue.vertexIndex(2));

        queue.resortVertex(1, 0.9);

        Assert.assertEquals(3, queue.vertexIndex(0));
        Assert.assertEquals(2, queue.vertexIndex(1));
        Assert.assertEquals(1, queue.vertexIndex(2));

        Assert.assertEquals(-1, queue.nextNotCheckedVertexIndex());

        queue.resortVertex(1, 1.1);

        Assert.assertEquals(3, queue.vertexIndex(0));
        Assert.assertEquals(2, queue.vertexIndex(1));
        Assert.assertEquals(1, queue.vertexIndex(2));

        Assert.assertEquals(-1, queue.nextNotCheckedVertexIndex());

        queue.resortVertex(2, 0.75);

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
            var distance = 10 * rnd.nextDouble();
            var isPqDistance = rnd.nextBoolean();

            if (treeSet.add(new IntDoubleImmutablePair(vertexIndex, distance))) {
                queue.add(vertexIndex, distance, isPqDistance);
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
            var newDistance = 10 * rnd.nextDouble();

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
