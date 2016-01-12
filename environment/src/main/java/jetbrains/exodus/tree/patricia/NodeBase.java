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
package jetbrains.exodus.tree.patricia;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.tree.INode;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.PrintStream;

@SuppressWarnings({"ProtectedField"})
abstract class NodeBase implements INode {

    @NotNull
    protected ByteIterable keySequence;
    @Nullable
    protected ByteIterable value;

    NodeBase(@NotNull final ByteIterable keySequence, @Nullable final ByteIterable value) {
        this.keySequence = keySequence;
        this.value = value;
    }

    /**
     * Does specified iterator match node's key sequence?
     *
     * @param it iterator.
     * @return If parameter matches the key sequence then returns a non-negative value, otherwise - a negative one.
     * In the latter case, absolute value  of result is equal to the number of matching bytes plus one.
     * E.g,. if 0 bytes match (key sequence is empty) then result is 0, if 0 bytes do not match result is -1.
     */
    MatchResult matchesKeySequence(@NotNull final ByteIterator it) {
        int matchingLength = 0;
        final ByteIterator keyIt = keySequence.iterator();
        while (keyIt.hasNext()) {
            final byte keyByte = keyIt.next();
            if (!it.hasNext()) {
                return new MatchResult(-matchingLength - 1, keyByte, false, (byte) 0);
            }
            final byte nextByte = it.next();
            if (nextByte != keyByte) {
                return new MatchResult(-matchingLength - 1, keyByte, true, nextByte);
            }
            ++matchingLength;
        }
        return new MatchResult(matchingLength);
    }

    boolean hasKey() {
        return keySequence.getLength() > 0;
    }

    @NotNull
    @Override
    public ByteIterable getKey() {
        return keySequence;
    }

    @Override
    public boolean hasValue() {
        return value != null;
    }

    @Override
    @Nullable
    public ByteIterable getValue() {
        return value;
    }

    @Override
    public void dump(PrintStream out, int level, @Nullable ToString renderer) {
        throw new UnsupportedOperationException();
    }

    abstract long getAddress();

    abstract boolean isMutable();

    abstract MutableNode getMutableCopy(@NotNull final PatriciaTreeMutable mutableTree);

    abstract NodeBase getChild(@NotNull final PatriciaTreeBase tree, final byte b);

    @NotNull
    abstract NodeChildrenIterator getChildren(final byte b);

    @NotNull
    abstract NodeChildrenIterator getChildrenRange(final byte b);

    @NotNull
    abstract NodeChildrenIterator getChildrenLast();

    @NotNull
    abstract NodeChildren getChildren();

    abstract int getChildrenCount();

    @Override
    public String toString() {
        return String.format("%s} %s %s",
                keySequence.iterator().hasNext() ? "{key:" + keySequence.toString() : '{',
                value == null ? "@" : value.toString() + " @", getAddress()
        );
    }

    static void indent(PrintStream out, int level) {
        for (int i = 0; i < level; i++) {
            out.print(' ');
        }
    }

    @SuppressWarnings({"PackageVisibleField"})
    static class MatchResult {

        final int matchingLength;
        final byte keyByte;
        final boolean hasNext;
        final byte nextByte;

        MatchResult(final int matchingLength) {
            this(matchingLength, (byte) 0, false, (byte) 0);
        }

        private MatchResult(final int matchingLength, final byte keyByte, final boolean hasNext, final byte nextByte) {
            this.matchingLength = matchingLength;
            this.keyByte = keyByte;
            this.hasNext = hasNext;
            this.nextByte = nextByte;
        }
    }

    final class EmptyNodeChildrenIterator implements NodeChildrenIterator {

        @Override
        public boolean isMutable() {
            return false;
        }

        @Override
        public void nextInPlace() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void prevInPlace() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChildReference getNode() {
            return null;
        }

        @Override
        public NodeBase getParentNode() {
            return NodeBase.this;
        }

        @Override
        public int getIndex() {
            return 0;
        }

        @Override
        public ByteIterable getKey() {
            return ByteIterable.EMPTY;
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public ChildReference next() {
            return null;
        }

        @Override
        public boolean hasPrev() {
            return false;
        }

        @Override
        public ChildReference prev() {
            return null;
        }

        @Override
        public void remove() {
        }
    }
}