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
package jetbrains.exodus.tree;

import jetbrains.exodus.ByteIterable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class TreeCursor implements ITreeCursor {

    @NotNull
    final TreeTraverser traverser;
    protected boolean canGoDown;
    boolean alreadyIn;
    boolean inited;

    public TreeCursor(@NotNull final TreeTraverser traverser) {
        this(traverser, false);
    }

    public TreeCursor(@NotNull final TreeTraverser traverser, boolean alreadyIn) {
        this.traverser = traverser;
        this.alreadyIn = alreadyIn;
        canGoDown = true;
        inited = false;
    }

    protected boolean hasNext() {
        if (inited) {
            return traverser.canMoveRight() || advance();
        }
        return traverser.isNotEmpty();
    }

    protected boolean hasPrev() {
        if (inited) {
            return traverser.canMoveLeft() || retreat();
        }
        return traverser.isNotEmpty();
    }

    @Override
    public boolean getNext() {
        if (!inited) {
            traverser.init(true);
            inited = true;
        }
        if (alreadyIn) {
            alreadyIn = false;
            return true;
        }
        final boolean result = moveToNext();
        if (!result) {
            traverser.init(false);
            canGoDown = true;
            moveToPrev();
        }
        return result;
    }

    @Override
    public boolean getPrev() {
        if (!inited) {
            traverser.init(false);
            inited = true;
        }
        if (alreadyIn) {
            alreadyIn = false;
            return true;
        }
        final boolean result = moveToPrev();
        if (!result) {
            traverser.init(true);
            canGoDown = true;
            moveToNext();
        }
        return result;
    }

    @Override
    public boolean getLast() {
        // move up to root
        while (traverser.canMoveUp()) {
            traverser.moveUp();
        }
        traverser.init(false);
        inited = true;
        alreadyIn = !traverser.isNotEmpty() && traverser.hasValue();
        return getPrev();
    }

    @Override
    @NotNull
    public ByteIterable getKey() {
        return traverser.getKey();
    }

    @Override
    @NotNull
    public ByteIterable getValue() {
        return traverser.getValue();
    }

    @Override
    public boolean getNextDup() {
        // tree without duplicates can has next dup only in -1 position
        return traverser.getKey() == ByteIterable.EMPTY && getNext();
    }

    @Override
    public boolean getNextNoDup() {
        return getNext();
    }

    @Override
    public boolean getPrevDup() {
        return getPrev();
    }

    @Override
    public boolean getPrevNoDup() {
        return getPrev();
    }

    @Override
    public ByteIterable getSearchKey(@NotNull ByteIterable key) {
        return moveTo(key, null, false);
    }

    @Override
    public ByteIterable getSearchKeyRange(@NotNull ByteIterable key) {
        return moveTo(key, null, true);
    }

    @Override
    public boolean getSearchBoth(@NotNull ByteIterable key, @NotNull ByteIterable value) {
        return moveTo(key, value, false) != null;
    }

    @Override
    public ByteIterable getSearchBothRange(@NotNull ByteIterable key, @NotNull ByteIterable value) {
        return moveTo(key, value, true);
    }

    @Override
    public int count() {
        return 1;
    }

    @Override
    public void close() {
    }

    @Override
    public boolean deleteCurrent() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isMutable() {
        return false;
    }

    @Override
    public ITree getTree() {
        return traverser.getTree();
    }

    @Nullable
    protected ByteIterable moveTo(@NotNull ByteIterable key, @Nullable ByteIterable value, boolean rangeSearch) {
        if (rangeSearch ? traverser.moveToRange(key, value) : traverser.moveTo(key, value)) {
            canGoDown = true;
            alreadyIn = false;
            inited = true;
            return traverser.getValue();
        }

        return null;
    }

    private boolean moveToNext() {
        while (true) {
            if (canGoDown) {
                if (traverser.canMoveDown()) {
                    if (traverser.moveDown().hasValue()) {
                        return true;
                    }
                    continue;
                }
            } else {
                canGoDown = true;
            }
            if (traverser.canMoveRight()) {
                final INode node = traverser.moveRight();
                if (!traverser.canMoveDown() && node.hasValue()) {
                    return true;
                }
            } else if (!advance()) {
                break;
            }
        }
        return false;
    }

    private boolean moveToPrev() {
        while (true) {
            if (canGoDown) {
                if (traverser.canMoveDown()) {
                    if (traverser.moveDownToLast().hasValue()) {
                        return true;
                    }
                    continue;
                }
            } else {
                canGoDown = true;
            }
            if (traverser.canMoveLeft()) {
                final INode node = traverser.moveLeft();
                if (!traverser.canMoveDown() && node.hasValue()) {
                    return true;
                }
            } else if (!retreat()) {
                break;
            }
        }
        return false;
    }

    private boolean advance() {
        while (traverser.canMoveUp()) {
            if (traverser.canMoveRight()) {
                return true;
            } else {
                traverser.moveUp();
                canGoDown = false;
            }
        }

        return traverser.canMoveRight();
    }

    private boolean retreat() {
        while (traverser.canMoveUp()) {
            if (traverser.canMoveLeft()) {
                return true;
            } else {
                traverser.moveUp();
                canGoDown = false;
            }
        }

        return traverser.canMoveLeft();
    }
}
