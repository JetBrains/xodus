/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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
package jetbrains.exodus.tree.btree;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.log.Loggable;
import jetbrains.exodus.tree.INode;
import jetbrains.exodus.tree.LongIterator;
import org.jetbrains.annotations.NotNull;

import java.io.PrintStream;

/**
 */
interface ILeafNode extends INode {

    /**
     * Retruns leaf address. May return Loggable.NULL_ADDRESS if leaf wasn't saved yet.
     *
     * @return
     */
    long getAddress();

    /**
     * @param iterable
     * @return
     */
    int compareKeyTo(@NotNull final ByteIterable iterable);

    /**
     * @param iterable
     * @return
     */
    int compareValueTo(@NotNull final ByteIterable iterable);

    /**
     * Returns true if given value equals to node value or exists in dup subtree
     *
     * @param value
     * @return
     */
    boolean valueExists(ByteIterable value);

    /**
     * Iterator over all addresses that compose this node
     *
     * @return
     */
    LongIterator addressIterator();

    /**
     * True for node that store duplicates subtree
     *
     * @return
     */
    boolean isDup();

    /**
     * True for mutable node
     *
     * @return
     */
    boolean isMutable();

    @NotNull
    BTreeBase getTree();

    /**
     * Returns number of duplicates stored in this node. 1 for node without duplicates.
     *
     * @return
     */
    long getDupCount();

    boolean isDupLeaf();

    ILeafNode EMPTY = new ILeafNode() {
        @Override
        public boolean hasValue() {
            return false;
        }

        @NotNull
        @Override
        public ByteIterable getKey() {
            return ByteIterable.EMPTY;
        }

        @Override
        public ByteIterable getValue() {
            return ByteIterable.EMPTY;
        }

        @Override
        public long getAddress() {
            return Loggable.NULL_ADDRESS;
        }

        @Override
        public int compareKeyTo(@NotNull ByteIterable iterable) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int compareValueTo(@NotNull ByteIterable iterable) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean valueExists(ByteIterable value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LongIterator addressIterator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isDup() {
            return false;
        }

        @Override
        public boolean isMutable() {
            return false;
        }

        @NotNull
        @Override
        public BTreeBase getTree() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getDupCount() {
            return 0;
        }

        @Override
        public boolean isDupLeaf() {
            return false;
        }

        @Override
        public void dump(PrintStream out, int level, ToString renderer) {
            out.println("Empty leaf node");
        }
    };

}
