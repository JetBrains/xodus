/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.tree.patricia;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.log.DataIterator;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.RandomAccessLoggable;
import jetbrains.exodus.tree.INode;
import jetbrains.exodus.tree.ITree;
import jetbrains.exodus.tree.LongIterator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.PrintStream;

public abstract class PatriciaTreeBase implements ITree {

    /**
     * Loggable types describing patricia tree nodes.
     * All types start from the NODE_WO_KEY_WO_VALUE_WO_CHILDREN which corresponds to a non-root node without key,
     * without value, without children and without back reference. All other patricia loggables' types are made
     * using additional 5 bits, giving additional 31 types. So maximum value of a patricia loggable type is 43.
     */
    public static final byte MAX_VALID_LOGGABLE_TYPE = 43;
    public static final byte NODE_WO_KEY_WO_VALUE_WO_CHILDREN = 12;
    public static final byte HAS_KEY_BIT = 1;
    public static final byte HAS_VALUE_BIT = 2;
    public static final byte HAS_CHILDREN_BIT = 4;
    public static final byte ROOT_BIT = 8;
    public static final byte ROOT_BIT_WITH_BACKREF = 16;

    @NotNull
    protected final Log log;
    @NotNull
    private final DataIterator dataIterator;
    protected final int structureId;
    protected long size;

    protected PatriciaTreeBase(@NotNull final Log log, final int structureId) {
        this.log = log;
        dataIterator = new DataIterator(log);
        this.structureId = structureId;
    }

    @NotNull
    @Override
    public Log getLog() {
        return log;
    }

    @NotNull
    @Override
    public DataIterator getDataIterator(long address) {
        dataIterator.checkPage(address);
        return dataIterator;
    }

    @Override
    public int getStructureId() {
        return structureId;
    }

    @Override
    @Nullable
    public ByteIterable get(@NotNull final ByteIterable key) {
        final NodeBase node = getNode(key);
        return node == null ? null : node.getValue();
    }

    @Override
    public boolean hasPair(@NotNull final ByteIterable key, @NotNull final ByteIterable value) {
        final ByteIterable val = get(key);
        return val != null && val.compareTo(value) == 0;
    }

    @Override
    public boolean hasKey(@NotNull final ByteIterable key) {
        return get(key) != null;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public long getSize() {
        return size;
    }

    @Override
    public void dump(PrintStream out) {
        dump(out, null);
    }

    @Override
    public void dump(PrintStream out, INode.ToString renderer) {
        new TreeAwareNodeDecorator(this, getRoot()).dump(out, 0, renderer);
    }

    @Override
    public LongIterator addressIterator() {
        if (isEmpty()) {
            return LongIterator.EMPTY;
        }
        return new AddressIterator(new PatriciaTraverser(this, getRoot()));
    }

    @NotNull
    final RandomAccessLoggable getLoggable(final long address) {
        return log.readNotNull(getDataIterator(address), address);
    }

    @NotNull
    final ImmutableNode loadNode(final long address) {
        final RandomAccessLoggable loggable = getLoggable(address);
        return new ImmutableNode(address, loggable.getType(), loggable.getData());
    }

    static boolean nodeHasKey(final byte type) {
        return ((type - NODE_WO_KEY_WO_VALUE_WO_CHILDREN) & HAS_KEY_BIT) != 0;
    }

    static boolean nodeHasValue(final byte type) {
        return ((type - NODE_WO_KEY_WO_VALUE_WO_CHILDREN) & HAS_VALUE_BIT) != 0;
    }

    static boolean nodeHasChildren(final byte type) {
        return ((type - NODE_WO_KEY_WO_VALUE_WO_CHILDREN) & HAS_CHILDREN_BIT) != 0;
    }

    static boolean nodeIsRoot(final byte type) {
        return ((type - NODE_WO_KEY_WO_VALUE_WO_CHILDREN) & ROOT_BIT) != 0;
    }

    static boolean nodeHasBackReference(final byte type) {
        return ((type - NODE_WO_KEY_WO_VALUE_WO_CHILDREN) & ROOT_BIT_WITH_BACKREF) != 0;
    }

    abstract NodeBase getRoot();

    @Nullable
    protected NodeBase getNode(@NotNull final ByteIterable key) {
        final ByteIterator it = key.iterator();
        NodeBase node = getRoot();
        do {
            if (NodeBase.MatchResult.getMatchingLength(node.matchesKeySequence(it)) < 0) {
                return null;
            }
            if (!it.hasNext()) {
                break;
            }
            node = node.getChild(this, it.next());
        } while (node != null);
        return node;
    }
}
