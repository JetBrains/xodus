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
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.log.ByteIterableWithAddress;
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.PrintStream;

class InternalPage extends BasePageImmutable {

    private byte childAddressLen;

    protected InternalPage(@NotNull final BTreeBase tree, @NotNull final ByteIterableWithAddress data) {
        super(tree, data);
    }

    protected InternalPage(@NotNull final BTreeBase tree, @NotNull final ByteIterableWithAddress data, int size) {
        super(tree, data, size);
    }

    @Override
    protected void loadAddressLengths(final int length) {
        super.loadAddressLengths(length);
        final ByteIterator it = getDataIterator(0);
        it.skip(size * keyAddressLen);
        checkAddressLength(childAddressLen = it.next());
    }

    @Override
    @NotNull
    protected BasePageMutable getMutableCopy(BTreeMutable treeMutable) {
        return new InternalPageMutable(treeMutable, this);
    }

    @Override
    public long getChildAddress(final int index) {
        final int offset = size * keyAddressLen + index * childAddressLen + 1;
        return data.nextLong((int) (dataAddress - data.getDataAddress() + offset), childAddressLen);
    }

    @Override
    @NotNull
    public BasePage getChild(final int index) {
        return getTree().loadPage(getChildAddress(index));
    }

    @Override
    protected boolean isBottom() {
        return false;
    }

    @Override
    public ILeafNode get(@NotNull final ByteIterable key) {
        return get(key, this);
    }

    @Override
    public ILeafNode find(@NotNull BTreeTraverser stack, int depth, @NotNull ByteIterable key, @Nullable ByteIterable value, boolean equalOrNext) {
        return find(stack, depth, key, value, equalOrNext, this);
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
        final int index = binarySearchGuessUnsafe(this, key);
        return index >= 0 && (getChildAddress(index) == pageAddress || getChild(index).childExists(key, pageAddress));
    }

    static ILeafNode get(@NotNull final ByteIterable key, BasePage page) {
        final int index = page.binarySearch(key);
        return index < 0 ? page.getChild(Math.max(-index - 2, 0)).get(key) : page.getKey(index);
    }

    @SuppressWarnings({"VariableNotUsedInsideIf"})
    @Nullable
    static ILeafNode find(@NotNull BTreeTraverser stack, int depth, @NotNull ByteIterable key, @Nullable ByteIterable value, boolean equalOrNext, @NotNull BasePage page) {
        int index = binarySearchGuess(page, key);
        ILeafNode ln = page.getChild(index).find(stack, depth + 1, key, value, equalOrNext);
        if (ln == null && value == null && equalOrNext) {
            // try next child
            if (index < page.getSize() - 1) {
                ++index;
                ln = page.getChild(index).find(stack, depth + 1, key, value, equalOrNext);
            }
        }

        if (ln != null) {
            stack.setAt(depth, new TreePos(page, index));
        }

        return ln;
    }

    static boolean keyExists(@NotNull final ByteIterable key, BasePage page) {
        final int index = page.binarySearch(key);
        return index >= 0 || page.getChild(Math.max(-index - 2, 0)).keyExists(key);
    }

    static boolean exists(@NotNull final ByteIterable key, @NotNull final ByteIterable value, BasePage page) {
        final int index = page.binarySearch(key);
        return index < 0 ? page.getChild(Math.max(-index - 2, 0)).exists(key, value) : page.getKey(index).valueExists(value);
    }

    @Override
    protected long getBottomPagesCount() {
        long result = 0;
        for (int i = 0; i < getSize(); i++) {
            result += getChild(i).getBottomPagesCount();
        }
        return result;
    }

    @Override
    public String toString() {
        return "Internal [" + size + "] @ " + (getDataAddress() - CompressedUnsignedLongByteIterable.getIterable(size << 1).getLength() - 2);
    }

    @Override
    public void dump(PrintStream out, int level, ToString renderer) {
        dump(out, level, renderer, this);
    }

    protected void reclaim(final ByteIterable keyIterable, @NotNull final BTreeReclaimTraverser context) {
        if (context.currentNode.getDataAddress() != getDataAddress()) {
            // go up
            if (context.canMoveUp()) {
                while (true) {
                    context.popAndMutate();
                    if (context.currentNode.getDataAddress() == getDataAddress()) {
                        doReclaim(context);
                        return;
                    }
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
                if (context.currentNode.getDataAddress() == getDataAddress()) {
                    doReclaim(context);
                    return;
                }
                int index = context.getNextSibling(keyIterable);
                if (index < 0) {
                    index = Math.max(-index - 2, 0);
                }
                context.pushChild(index);
            }
        } else {
            doReclaim(context);
        }
    }

    /*
     * Returns safe binary search.
     * @return index (non-negative result is guaranteed)
     */
    protected static int binarySearchGuess(@NotNull final BasePage page, @NotNull final ByteIterable key) {
        int index = binarySearchGuessUnsafe(page, key);
        if (index < 0) index = 0;
        return index;
    }

    /*
     * Returns unsafe binary search index.
     * @return index (non-negative or -1 which means that nothing was found)
     */
    protected static int binarySearchGuessUnsafe(@NotNull final BasePage page, @NotNull final ByteIterable key) {
        int index = page.binarySearch(key);
        if (index < 0) {
            index = -index - 2;
        }
        return index;
    }

    static void dump(PrintStream out, int level, ToString renderer, BasePage page) {
        indent(out, level);
        out.println(page);
        for (int i = 0; i < page.getSize(); i++) {
            indent(out, level);
            out.print("+");
            page.getKey(i).dump(out, 0, renderer);
            page.getChild(i).dump(out, level + 3, renderer);
        }
    }
}
