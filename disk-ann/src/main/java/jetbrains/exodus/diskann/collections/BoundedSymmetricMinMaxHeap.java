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
package jetbrains.exodus.diskann.collections;

import org.apache.commons.lang3.ArrayUtils;
import org.jetbrains.annotations.NotNull;

public final class BoundedSymmetricMinMaxHeap {
    private final long[] tree;
    private int size;

    public BoundedSymmetricMinMaxHeap(int capacity) {
        this.tree = new long[2 * capacity + 2];
    }

    public void add(long vertexIndex, double distance) {
        size++;

        var current = size;

        var currentIndex = arrayIndex(current);
        tree[currentIndex] = vertexIndex;
        tree[currentIndex + 1] = Double.doubleToLongBits(distance);

        adjustTreeAfterInsertion(current);

        if (size > (tree.length >> 1) - 1) {
            removeMax();
        }
    }

    public int size() {
        return size;
    }

    private void adjustTreeAfterInsertion(int current) {
        while (true) {
            var tempCurrent = current;
            tempCurrent = adjustSibling(tempCurrent);
            tempCurrent = adjustGrandParent(tempCurrent);
            if (tempCurrent == current) {
                break;
            }
            current = tempCurrent;
        }
    }

    private void adjustTreeAfterDeletion(int current) {
        while (true) {
            adjustSibling(current);

            var tempCurrent = current;
            tempCurrent = adjustGrandChild(tempCurrent);
            if (tempCurrent == current) {
                break;
            }
            current = tempCurrent;
        }
    }

    public void removeMax() {
        if (size == 0) {
            throw new IllegalStateException("Heap is empty");
        }

        if (size == 1) {
            size--;
            return;
        }

        deleteIndex(2);
    }

    @Override
    public String toString() {
        var builder = new StringBuilder();
        if (size == 0) {
            builder.append("{}");
        } else {
            builder.append("{ size: ").append(size).append("; array : [");
            for (int i = 0; i < size; i++) {
                builder.append("(").append(tree[i << 1]).append(", ").append(Double.longBitsToDouble(tree[(i << 1) + 1])).append(")");
                if (i != size - 1) {
                    builder.append(",");
                }
            }
            builder.append("]}");
        }

        return builder.toString();
    }

    @NotNull
    public long[] removeMin() {
        if (size == 0) {
            throw new IllegalStateException("Heap is empty");
        }

        long[] deleted = new long[]{tree[0], tree[1]};

        if (size == 1) {
            size--;
            return deleted;
        }

        deleteIndex(1);

        if (size == 1) {
            tree[0] = tree[2];
            tree[1] = tree[3];
        }

        return deleted;
    }

    private void deleteIndex(int current) {
        var currentIndex = arrayIndex(current);
        var lastIndex = arrayIndex(size);

        tree[currentIndex] = tree[lastIndex];
        tree[currentIndex + 1] = tree[lastIndex + 1];

        size--;

        adjustTreeAfterDeletion(current);

    }

    private int adjustSibling(int current) {
        int sibling;
        if ((current & 1) == 1) {
            if (current == size) {
                return current;
            }

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

    private int adjustGrandParent(int current) {
        if (current <= 2) {
            return current;
        }

        int grandParent = (((current - 1) >> 1) - 1) >> 1;
        int leftGrandParentChild = (grandParent << 1) + 1;
        int rightGrandParentChild = leftGrandParentChild + 1;

        int currentIndex = arrayIndex(current);
        int leftGrandParentChildIndex = arrayIndex(leftGrandParentChild);
        int rightGrandParentChildIndex = arrayIndex(rightGrandParentChild);

        if (Double.longBitsToDouble(tree[leftGrandParentChildIndex + 1]) > Double.longBitsToDouble(tree[currentIndex + 1])) {
            swap(current, leftGrandParentChild);
            return leftGrandParentChild;
        } else if (Double.longBitsToDouble(tree[rightGrandParentChildIndex + 1]) < Double.longBitsToDouble(tree[currentIndex + 1])) {
            swap(current, rightGrandParentChild);
            return rightGrandParentChild;
        }

        return current;
    }

    private int adjustGrandChild(int current) {
        var leftChild = (current << 1) + 1;
        if ((current & 1) == 1) {
            if (leftChild > size) {
                return current;
            }

            var sibling = current + 1;
            var rightSiblingLeftChild = (sibling << 1) + 1;
            int child = leftChild;

            if (rightSiblingLeftChild <= size) {
                if (Double.longBitsToDouble(tree[arrayIndex(rightSiblingLeftChild) + 1]) < Double.longBitsToDouble(tree[arrayIndex(leftChild) + 1])) {
                    child = rightSiblingLeftChild;
                }
            }

            if (Double.longBitsToDouble(tree[arrayIndex(child) + 1]) < Double.longBitsToDouble(tree[arrayIndex(current) + 1])) {
                swap(current, child);
                return child;
            }
        } else {
            var rightChild = leftChild + 1;

            var sibling = current - 1;
            int child;
            if (rightChild <= size) {
                var leftSiblingRightChild = (sibling << 1) + 2;
                if (Double.longBitsToDouble(tree[arrayIndex(leftSiblingRightChild) + 1]) > Double.longBitsToDouble(tree[arrayIndex(rightChild) + 1])) {
                    child = leftSiblingRightChild;
                } else {
                    child = rightChild;
                }
            } else if (leftChild <= size) {
                var leftSiblingRightChild = (sibling << 1) + 2;

                if (Double.longBitsToDouble(tree[arrayIndex(leftSiblingRightChild) + 1]) > Double.longBitsToDouble(tree[arrayIndex(leftChild) + 1])) {
                    child = leftSiblingRightChild;
                } else {
                    child = leftChild;
                }
            } else {
                var leftSiblingLeftChild = (sibling << 1) + 1;
                var leftSiblingRightChild = leftSiblingLeftChild + 1;

                if (leftSiblingRightChild <= size) {
                    child = leftSiblingRightChild;
                } else if (leftSiblingLeftChild <= size) {
                    child = leftSiblingLeftChild;
                } else {
                    return current;
                }
            }

            if (Double.longBitsToDouble(tree[arrayIndex(child) + 1]) > Double.longBitsToDouble(tree[arrayIndex(current) + 1])) {
                swap(current, child);
                return child;
            }
        }

        return current;
    }

    private int arrayIndex(int treeIndex) {
        return (treeIndex - 1) << 1;
    }

    private void swap(int firstIndex, int secondIndex) {
        var firstArrayIndex = arrayIndex(firstIndex);
        var secondArrayIndex = arrayIndex(secondIndex);

        ArrayUtils.swap(tree, firstArrayIndex, secondArrayIndex);
        ArrayUtils.swap(tree, firstArrayIndex + 1, secondArrayIndex + 1);
    }
}
