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

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.tree.ITree;
import jetbrains.exodus.tree.ITreeMutable;
import jetbrains.exodus.tree.TreeBaseTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static junit.framework.Assert.assertEquals;

public class PatriciaTestBase extends TreeBaseTest {
    @Override
    public PatriciaTreeBase getTree() {
        return (PatriciaTreeBase) super.getTree();
    }

    @Override
    public PatriciaTreeMutable getTreeMutable() {
        return (PatriciaTreeMutable) super.getTreeMutable();
    }

    @Override
    protected ITreeMutable createMutableTree(final boolean hasDuplicates, final int structureId) {
        return doCreateMutableTree(hasDuplicates, structureId);
    }

    @Override
    protected ITree openTree(long address, boolean hasDuplicates) {
        return doOpenTree(address, hasDuplicates);
    }

    protected static ITreeMutable doCreateMutableTree(final boolean hasDuplicates, final int structureId) {
        return new PatriciaTreeEmpty(log, structureId, hasDuplicates).getMutableCopy();
    }

    protected static ITree doOpenTree(long address, boolean hasDuplicates) {
        final PatriciaTree tree = new PatriciaTree(log, address, 1);
        return hasDuplicates ? new PatriciaTreeWithDuplicates(tree) : tree;
    }

    public void assertMatches(@NotNull final ITree t, @NotNull final N node) {
        final PatriciaTreeBase tree = (PatriciaTreeBase) t;
        node.matches(tree, tree.getRoot());
    }

    public static N R(N... expectedChildren) {
        return new N(false, expectedChildren);
    }

    public static N R(String key, N... expectedChildren) {
        return R(key, null, expectedChildren);
    }

    public static N R(String key, @Nullable String value, N... expectedChildren) {
        return new N(false, new ArrayByteIterable(key.getBytes()),
                value == null ? null : new ArrayByteIterable(value.getBytes()), expectedChildren);
    }

    public static N KN(char c, String key, N... expectedChildren) {
        return N(c, key, null, expectedChildren);
    }

    public static N N(char c, String key, @Nullable String value, N... expectedChildren) {
        return new N(false, c, new ArrayByteIterable(key.getBytes()),
                value == null ? null : new ArrayByteIterable(value.getBytes()), expectedChildren);
    }

    public static N N(char c, N... expectedChildren) {
        return N(c, "", null, expectedChildren);
    }

    public static N N(char c, String key, N... expectedChildren) {
        return N(c, key, null, expectedChildren);
    }

    public static N RM(N... expectedChildren) {
        return new N(true, expectedChildren);
    }

    public static N RM(String key, N... expectedChildren) {
        return RM(key, null, expectedChildren);
    }

    public static N RM(String key, @Nullable String value, N... expectedChildren) {
        return new N(true, new ArrayByteIterable(key.getBytes()),
                value == null ? null : new ArrayByteIterable(value.getBytes()), expectedChildren);
    }

    public static N KNM(char c, String key, N... expectedChildren) {
        return NM(c, key, null, expectedChildren);
    }

    public static N NM(char c, String key, @Nullable String value, N... expectedChildren) {
        return new N(true, c, new ArrayByteIterable(key.getBytes()),
                value == null ? null : new ArrayByteIterable(value.getBytes()), expectedChildren);
    }

    public static N NM(char c, N... expectedChildren) {
        return NM(c, "", null, expectedChildren);
    }

    public static N NM(char c, String key, N... expectedChildren) {
        return NM(c, key, null, expectedChildren);
    }

    public static class N {

        char c;
        N[] children;
        @NotNull
        ByteIterable keySequence;
        @Nullable
        ByteIterable value;
        boolean mutable;

        N(boolean isMutable, N... expectedChildren) {
            this(isMutable, '*', expectedChildren);
        }

        N(boolean isMutable, ByteIterable expectedKey, N... expectedChildren) {
            this(isMutable, '*', expectedKey, expectedChildren);
        }

        N(boolean isMutable, ByteIterable expectedKey, ByteIterable expectedValue, N... expectedChildren) {
            this(isMutable, '*', expectedKey, expectedValue, expectedChildren);
        }

        N(boolean isMutable, char expectedChar, N... expectedChildren) {
            this(isMutable, expectedChar, ByteIterable.EMPTY, expectedChildren);
        }

        N(boolean isMutable, char expectedChar, ByteIterable expectedKey, N... expectedChildren) {
            this(isMutable, expectedChar, expectedKey, null, expectedChildren);
        }

        N(boolean isMutable, char expectedChar, ByteIterable expectedKey, @Nullable ByteIterable expectedValue, N... expectedChildren) {
            mutable = isMutable;
            c = expectedChar;
            children = expectedChildren;
            keySequence = expectedKey;
            value = expectedValue;
        }

        void matches(PatriciaTreeBase tree, NodeBase node) {
            assertEquals(mutable, node.isMutable());
            assertEquals(children.length, node.getChildrenCount());
            assertIterablesMatch(keySequence, node.keySequence);
            assertIterablesMatch(value, node.value);
            int i = 0;
            for (ChildReference ref : node.getChildren()) {
                final N expectedChild = children[i];
                assertEquals(expectedChild.c, ref.firstByte);
                NodeBase child = ref.getNode(tree);
                expectedChild.matches(tree, child);
                ++i;
            }
        }

    }
}
