package jetbrains.exodus.tree.ibtree;

import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

interface MutablePage extends TraversablePage {
    ByteBuffer key(int index);

    int find(ByteBuffer key);

    long save(int structureId, @Nullable MutableInternalPage parent);

    @Nullable
    RebalanceResult rebalance(@Nullable MutableInternalPage parent, boolean rebalanceChildren);

    void spill(@Nullable MutableInternalPage parent);

    long treeSize();

    long address();

    boolean fetch();

    void merge(MutablePage page);

    void unbalance();
}