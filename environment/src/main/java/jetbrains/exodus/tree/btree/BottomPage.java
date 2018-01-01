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
import jetbrains.exodus.log.ByteIterableWithAddress;
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.PrintStream;

class BottomPage extends BasePageImmutable {

    BottomPage(@NotNull final BTreeBase tree) {
        super(tree);
    }

    protected BottomPage(@NotNull final BTreeBase tree, @NotNull final ByteIterableWithAddress data) {
        super(tree, data);
    }

    protected BottomPage(@NotNull final BTreeBase tree, @NotNull final ByteIterableWithAddress data, int size) {
        super(tree, data, size);
    }

    @Override
    protected boolean isBottom() {
        return true;
    }

    @Override
    public long getChildAddress(int index) {
        return getKeyAddress(index);
    }

    @Override
    protected long getBottomPagesCount() {
        return 1;
    }

    @Override
    public ILeafNode get(@NotNull ByteIterable key) {
        return get(key, this);
    }

    @Override
    public ILeafNode find(@NotNull BTreeTraverser stack, int depth, @NotNull ByteIterable key, @Nullable ByteIterable value, boolean equalOrNext) {
        return find(stack, depth, key, value, equalOrNext, this);
    }

    @Override
    @NotNull
    protected BasePageMutable getMutableCopy(BTreeMutable treeMutable) {
        return new BottomPageMutable(treeMutable, this);
    }

    @Override
    public boolean keyExists(@NotNull ByteIterable key) {
        return keyExists(key, this);
    }

    @Override
    public boolean exists(@NotNull ByteIterable key, @NotNull ByteIterable value) {
        return exists(key, value, this);
    }

    @Override
    public boolean childExists(@NotNull ByteIterable key, long pageAddress) {
        return false;
    }

    @Override
    public String toString() {
        return "Bottom [" + size + "] @ " + (getDataAddress() - CompressedUnsignedLongByteIterable.getIterable(size << 1).getLength() - 1);
    }

    @Override
    public void dump(PrintStream out, int level, ToString renderer) {
        dump(out, level, renderer, this);
    }

    protected void reclaim(final ByteIterable keyIterable, @NotNull final BTreeReclaimTraverser context) {
        final BasePage node = context.currentNode;
        if (node.isBottom()) {
            if (node.getDataAddress() == getDataAddress()) {
                doReclaim(context);
                return;
            } else if (node.size > 0 && node.getMinKey().compareKeyTo(keyIterable) == 0) {
                // TODO: if root is empty just use other code in tree reclaim instead, omitting "node.size > 0" check
                // we are already in desired bottom page, but address in not the same
                return;
            }
        }
        // go up
        if (context.canMoveUp()) {
            while (true) {
                context.popAndMutate();
                context.moveRight();
                final int index = context.getNextSibling(keyIterable);
                if (index < 0) {
                    if (!context.canMoveUp()) {
                        context.moveTo(Math.max(-index - 2, 0));
                        break;
                    }
                } else {
                    context.pushChild(index); // node is always internal
                    break;
                }
            }
        }
        // go down
        while (context.canMoveDown()) {
            int index = context.getNextSibling(keyIterable);
            if (index < 0) {
                index = Math.max(-index - 2, 0);
            }
            context.pushChild(index);
        }
        if (context.currentNode.getDataAddress() == getDataAddress()) {
            doReclaim(context);
        }
    }

    static ILeafNode get(@NotNull ByteIterable key, @NotNull BasePage page) {
        final int index = page.binarySearch(key);
        if (index >= 0) {
            return page.getKey(index);
        }
        return null;
    }

    @NotNull
    private static ILeafNode findFirst(@NotNull BTreeTraverser stack, int depth, BasePage page) {
        final ILeafNode result;
        if (page.isBottom()) {
            result = page.getMinKey();
            stack.currentNode = page;
            stack.currentPos = 0;
            stack.top = depth;
        } else {
            result = findFirst(stack, depth + 1, page.getChild(0));
            stack.setAt(depth, new TreePos(page, 0));
        }
        return result;
    }

    @Nullable
    static ILeafNode find(@NotNull BTreeTraverser stack, int depth, @NotNull ByteIterable key, @Nullable ByteIterable value, boolean equalOrNext, @NotNull BasePage page) {
        int index = page.binarySearch(key);
        if (index < 0) {
            if (value == null && equalOrNext) {
                index = -index - 1;
                if (index >= page.getSize()) return null; // after last element - no element to return
            } else {
                return null;
            }
        }

        ILeafNode ln = page.getKey(index);

        if (ln.isDup()) {
            BasePage dupRoot = ln.getTree().getRoot();
            ILeafNode dupLeaf;
            if (value != null) {
                // move dup cursor to requested value
                dupLeaf = dupRoot.find(stack, depth + 1, value, null, equalOrNext);
                if (dupLeaf == null) {
                    return null;
                }
            } else {
                dupLeaf = findFirst(stack, depth + 1, dupRoot);
            }

            stack.setAt(depth, new TreePos(page, index));

            ((BTreeTraverserDup) stack).inDupTree = true;

            return dupLeaf;
        }
        if (stack.isDup()) {
            ((BTreeTraverserDup) stack).inDupTree = false;
        }


        if (value == null || (equalOrNext ? value.compareTo(ln.getValue()) <= 0 : value.compareTo(ln.getValue()) == 0)) {
            stack.currentNode = page;
            stack.currentPos = index;
            stack.top = depth;
            //TODO: stack.bottom = 0?
            return ln;
        }

        return null;
    }

    static boolean keyExists(@NotNull ByteIterable key, @NotNull BasePage page) {
        return page.binarySearch(key) >= 0;
    }

    static boolean exists(@NotNull ByteIterable key, @NotNull ByteIterable value, @NotNull BasePage page) {
        ILeafNode ln = page.get(key);
        return ln != null && ln.valueExists(value);
    }

    static void dump(PrintStream out, int level, ToString renderer, BasePage page) {
        indent(out, level);
        out.println(page);
        for (int i = 0; i < page.getSize(); i++) {
            page.getKey(i).dump(out, level + 1, renderer);
        }
    }

}
