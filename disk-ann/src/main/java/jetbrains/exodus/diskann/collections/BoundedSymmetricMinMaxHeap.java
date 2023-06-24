package jetbrains.exodus.diskann.collections;

import org.apache.commons.lang3.ArrayUtils;

public class BoundedSymmetricMinMaxHeap {
    private final long[] tree;
    private int size;

    public BoundedSymmetricMinMaxHeap(int capacity) {
        if ((capacity & 1) == 1) {
            capacity++;
        }

        this.tree = new long[2 * capacity];
    }

    private int adjustSibling(int current) {
        int sibling;
        if ((current & 1) == 0) {
            //left child
            sibling = current + 1;

            var siblingIndex = arrayIndex(sibling);
            var currentIndex = arrayIndex(current);

            if (Double.longBitsToDouble(tree[siblingIndex + 1]) < Double.longBitsToDouble(tree[currentIndex + 1])) {
                swap(current, sibling);
                return sibling;
            }
        } else {
            //right child
            sibling = current - 1;

            var siblingIndex = arrayIndex(sibling);
            var currentIndex = arrayIndex(current);

            if (Double.longBitsToDouble(tree[siblingIndex + 1]) > Double.longBitsToDouble(tree[currentIndex + 1])) {
                swap(current, sibling);
                return sibling;
            }
        }

        return current;
    }

    private int arrayIndex(int treeIndex) {
        return treeIndex << 1;
    }

    private void swap(int firstIndex, int secondIndex) {
        var firstArrayIndex = arrayIndex(firstIndex);
        var secondArrayIndex = arrayIndex(secondIndex);

        ArrayUtils.swap(tree, firstArrayIndex, secondArrayIndex);
        ArrayUtils.swap(tree, firstArrayIndex + 1, secondArrayIndex + 1);
    }

}
