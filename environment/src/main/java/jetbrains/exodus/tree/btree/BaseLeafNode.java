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
package jetbrains.exodus.tree.btree;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.log.Loggable;
import jetbrains.exodus.tree.INode;
import jetbrains.exodus.tree.LongIterator;
import org.jetbrains.annotations.NotNull;

import java.io.PrintStream;

/**
 * Base implementation of leaf node
 */
abstract class BaseLeafNode implements ILeafNode {

    @Override
    public boolean equals(Object obj) {
        return this == obj || (obj instanceof INode && getKey().equals(((INode) obj).getKey()));
    }

    @Override
    public boolean hasValue() {
        return true;
    }

    @Override
    public boolean isDupLeaf() {
        return false;
    }

    @Override
    public int hashCode() {
        return getKey().hashCode();
    }

    @Override
    public long getAddress() {
        return Loggable.NULL_ADDRESS;
    }

    @Override
    @NotNull
    public BTreeBase getTree() {
        throw new UnsupportedOperationException();
    }

    @Override
    @NotNull
    public LongIterator addressIterator() {
        return LongIterator.EMPTY;
    }

    @Override
    public boolean isDup() {
        return false;
    }

    @Override
    public long getDupCount() {
        return 1;
    }

    @Override
    public boolean valueExists(@NotNull ByteIterable value) {
        return compareValueTo(value) == 0;
    }

    @Override
    public int compareKeyTo(@NotNull final ByteIterable iterable) {
        return getKey().compareTo(iterable);
    }

    @Override
    public int compareValueTo(@NotNull final ByteIterable iterable) {
        return getValue().compareTo(iterable);
    }

    @Override
    public String toString() {
        return "LN {key:" + getKey().toString() + "} @ " + getAddress();
    }

    @Override
    public void dump(PrintStream out, int level, ToString renderer) {
        BasePage.indent(out, level);
        out.println(renderer == null ? toString() : renderer.toString(this));
    }

}
