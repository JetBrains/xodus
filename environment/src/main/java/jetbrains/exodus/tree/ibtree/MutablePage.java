package jetbrains.exodus.tree.ibtree;

import jetbrains.exodus.tree.ExpiredLoggableCollection;

import java.nio.ByteBuffer;

interface MutablePage {
    ByteBuffer key(int index);

    int find(ByteBuffer key);

    long save(int structureId, ExpiredLoggableCollection expiredLoggables);

    void rebalance(ExpiredLoggableCollection loggables);

    void spill();
}