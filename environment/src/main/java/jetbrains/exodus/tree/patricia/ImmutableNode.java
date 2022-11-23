package jetbrains.exodus.tree.patricia;

import jetbrains.exodus.log.RandomAccessLoggable;
import jetbrains.exodus.tree.INode;

public interface ImmutableNode extends INode {
    RandomAccessLoggable getLoggable();

    long getAddress();

    NodeBase asNodeBase();
}
