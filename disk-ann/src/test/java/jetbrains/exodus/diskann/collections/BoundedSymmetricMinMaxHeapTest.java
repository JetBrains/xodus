package jetbrains.exodus.diskann.collections;

import it.unimi.dsi.fastutil.longs.LongDoubleImmutablePair;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class BoundedSymmetricMinMaxHeapTest {
    @Test
    public void testAddSingle() {
        var heap = new BoundedSymmetricMinMaxHeap(1);
        heap.add(10, 2.0);

        Assert.assertEquals(1, heap.size());
        Assert.assertArrayEquals(new long[]{10, Double.doubleToLongBits(2.0)}, heap.removeMin());

        Assert.assertEquals(0, heap.size());
    }

    @Test
    public void testAddTwoWithSingleCapacity() {
        var heap = new BoundedSymmetricMinMaxHeap(1);
        heap.add(10, 2.0);
        heap.add(1, 1.0);

        Assert.assertEquals(1, heap.size());
        Assert.assertArrayEquals(new long[]{1, Double.doubleToLongBits(1.0)}, heap.removeMin());

        Assert.assertEquals(0, heap.size());
    }

    @Test
    public void testAddTree() {
        var heap = new BoundedSymmetricMinMaxHeap(3);

        heap.add(10, 2.0);
        heap.add(1, 1.0);
        heap.add(12, 3.0);

        Assert.assertEquals(3, heap.size());
        Assert.assertArrayEquals(new long[]{1, Double.doubleToLongBits(1.0)}, heap.removeMin());

        Assert.assertEquals(2, heap.size());
        Assert.assertArrayEquals(new long[]{10, Double.doubleToLongBits(2.0)}, heap.removeMin());

        Assert.assertEquals(1, heap.size());
        Assert.assertArrayEquals(new long[]{12, Double.doubleToLongBits(3.0)}, heap.removeMin());

        Assert.assertEquals(0, heap.size());
    }

    @Test
    public void testAddFiveWithCapacityThree() {
        var heap = new BoundedSymmetricMinMaxHeap(3);

        heap.add(10, 2.0);
        heap.add(1, 5.0);
        heap.add(12, 3.0);
        heap.add(9, 4.0);
        heap.add(8, 1.0);


        Assert.assertEquals(3, heap.size());
        Assert.assertArrayEquals(new long[]{8, Double.doubleToLongBits(1.0)}, heap.removeMin());

        Assert.assertEquals(2, heap.size());
        Assert.assertArrayEquals(new long[]{10, Double.doubleToLongBits(2.0)}, heap.removeMin());

        Assert.assertEquals(1, heap.size());
        Assert.assertArrayEquals(new long[]{12, Double.doubleToLongBits(3.0)}, heap.removeMin());

        Assert.assertEquals(0, heap.size());
    }

    @Test
    public void testAddForeRemoveTwoCapacityThree() {
        var heap = new BoundedSymmetricMinMaxHeap(3);

        heap.add(10, 2.0);
        heap.add(1, 1.0);
        heap.add(12, 3.0);

        Assert.assertEquals(3, heap.size());
        Assert.assertArrayEquals(new long[]{1, Double.doubleToLongBits(1.0)}, heap.removeMin());

        Assert.assertEquals(2, heap.size());
        Assert.assertArrayEquals(new long[]{10, Double.doubleToLongBits(2.0)}, heap.removeMin());

        heap.add(9, 4.0);
        heap.add(8, 5.0);

        Assert.assertEquals(3, heap.size());
        Assert.assertArrayEquals(new long[]{12, Double.doubleToLongBits(3.0)}, heap.removeMin());

        Assert.assertEquals(2, heap.size());
        Assert.assertArrayEquals(new long[]{9, Double.doubleToLongBits(4.0)}, heap.removeMin());

        Assert.assertEquals(1, heap.size());
        Assert.assertArrayEquals(new long[]{8, Double.doubleToLongBits(5.0)}, heap.removeMin());

        Assert.assertEquals(0, heap.size());
    }

    @Test
    public void testRandomAdd12Capacity8RemoveMin() {
        var seed = System.nanoTime();
        System.out.printf("testRandomAdd12Capacity8RemoveMin: seed %d%n", seed);

        var heap = new BoundedSymmetricMinMaxHeap(8);
        var items = new TreeSet<>(Comparator.comparingDouble(LongDoubleImmutablePair::rightDouble));
        var rnd = new Random(seed);

        for (int i = 0; i < 12; i++) {
            var pair = new LongDoubleImmutablePair(rnd.nextLong(), rnd.nextDouble());
            if (items.add(pair)) {
                heap.add(pair.leftLong(), pair.rightDouble());
            }
        }

        Assert.assertEquals(8, heap.size());

        while (items.size() > 8) {
            items.pollLast();
        }

        while (!items.isEmpty()) {
            var item = items.pollFirst();
            Assert.assertNotNull(item);

            var heapItem = heap.removeMin();
            Assert.assertEquals(item.leftLong(), heapItem[0]);
            Assert.assertEquals(item.rightDouble(), Double.longBitsToDouble(heapItem[1]), 0.0);
        }
    }

    @Test
    public void testRandomAdd1024Capacity157RemoveMin() {
        var seed = System.nanoTime();
        System.out.printf("testRandomAdd1024Capacity157RemoveMin: seed %d%n", seed);

        var heap = new BoundedSymmetricMinMaxHeap(157);
        var items = new TreeSet<>(Comparator.comparingDouble(LongDoubleImmutablePair::rightDouble));
        var rnd = new Random(seed);

        for (int i = 0; i < 1024; i++) {
            var pair = new LongDoubleImmutablePair(rnd.nextLong(), rnd.nextDouble());
            if (items.add(pair)) {
                heap.add(pair.leftLong(), pair.rightDouble());
            }
        }

        Assert.assertEquals(157, heap.size());

        while (items.size() > 157) {
            items.pollLast();
        }

        while (!items.isEmpty()) {
            var item = items.pollFirst();
            Assert.assertNotNull(item);

            var heapItem = heap.removeMin();
            Assert.assertEquals(item.leftLong(), heapItem[0]);
            Assert.assertEquals(item.rightDouble(), Double.longBitsToDouble(heapItem[1]), 0.0);
        }
    }
}
