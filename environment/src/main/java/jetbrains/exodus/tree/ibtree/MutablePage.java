package jetbrains.exodus.tree.ibtree;

import java.nio.ByteBuffer;

interface MutablePage extends TraversablePage {
    ByteBuffer key(int index);

    int find(ByteBuffer key);

    long save(int structureId);

    MutablePage rebalance();

    void spill();

    long treeSize();

    long address();
}