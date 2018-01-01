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


    long matchesKeySequence(@NotNull final ByteIterator it) {
        int matchingLength = 0;
        final ByteIterator keyIt = keySequence.iterator();
        while (keyIt.hasNext()) {
            final byte keyByte = keyIt.next();
            if (!it.hasNext()) {
                return MatchResult.getMatchResult(-matchingLength - 1, keyByte, false, (byte) 0);
            }
            final byte nextByte = it.next();
            if (nextByte != keyByte) {
                return MatchResult.getMatchResult(-matchingLength - 1, keyByte, true, nextByte);
            }
            ++matchingLength;
        }
        return MatchResult.getMatchResult(matchingLength);
    }

    boolean hasKey() {
        return keySequence != ByteIterable.EMPTY && keySequence.getLength() > 0;
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

        static long getMatchResult(final int matchingLength) {
            return getMatchResult(matchingLength, (byte) 0, false, (byte) 0);
        }

        static long getMatchResult(final int matchingLength,
                                   final byte keyByte,
                                   final boolean hasNext,
                                   final byte nextByte) {
            long result = (((long) Math.abs(matchingLength)) << 18) + ((keyByte & 0xff) << 10) + ((nextByte & 0xff) << 2);
            if (matchingLength < 0) {
                result += 2;
            }
            if (hasNext) {
                ++result;
            }
            return result;
        }

        static int getMatchingLength(final long matchResult) {
            final int result = (int) (matchResult >> 18);
            return (matchResult & 2) == 0 ? result : -result;
        }

        static int getKeyByte(final long matchResult) {
            return (int) (matchResult >> 10) & 0xff;
        }

        static int getNextByte(final long matchResult) {
            return (int) (matchResult >> 2) & 0xff;
        }

        static boolean hasNext(final long matchResult) {
            return (matchResult & 1) != 0;
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