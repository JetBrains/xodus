/**
 * Copyright 2010 - 2014 JetBrains s.r.o.
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
import org.jetbrains.annotations.Nullable;

/**
 * Cursor with deleteCurrent support.
 * May be used for trees with duplicates or without ones.
 */
public class TreeCursorMutable extends TreeCursor implements ITreeCursorMutable {

    protected ITreeMutable tree;
    protected boolean wasDelete = false;
    @Nullable
    protected ByteIterable nextAfterRemovedKey = null;
    @Nullable
    protected ByteIterable nextAfterRemovedValue = null;

    public TreeCursorMutable(ITreeMutable tree, TreeTraverser traverser) {
        super(traverser);
        this.tree = tree;
    }

    public TreeCursorMutable(ITreeMutable tree, TreeTraverser traverser, boolean alreadyIn) {
        super(traverser, alreadyIn);
        this.tree = tree;
    }

    @Override
    public boolean getNext() {
        if (wasDelete) {
            wasDelete = false;
            // move to remembered next
            final ByteIterable key = nextAfterRemovedKey;
            if (key != null) {
                if (traverser.moveTo(key, tree.isAllowingDuplicates() ? nextAfterRemovedValue : null)) {
                    inited = true;
                    return true;
                }
                return false;
            } else {
                return false;
            }
        } else {
            return super.getNext();
        }
    }

    protected void reset(MutableTreeRoot newRoot) {
        traverser.reset(newRoot);
        canGoDown = true;
        alreadyIn = false;
        inited = false;
    }

    @Override
    protected boolean hasNext() {
        return wasDelete ? nextAfterRemovedKey != null : super.hasNext();
    }

    @Override
    public boolean getPrev() {
        if (wasDelete) {
            throw new UnsupportedOperationException();
        } else {
            return super.getPrev();
        }
    }

    @Override
    protected boolean hasPrev() {
        if (wasDelete) {
            throw new UnsupportedOperationException();
        } else {
            return super.hasPrev();
        }
    }

    @Override
    public boolean deleteCurrent() {
        if (wasDelete) {
            return false;
        }

        // delete and remember next
        final ByteIterable key = getKey();
        final ByteIterable value = getValue();
        if (getNext()) {
            nextAfterRemovedKey = traverser.getKey();
            nextAfterRemovedValue = traverser.getValue();
        } else {
            nextAfterRemovedKey = null;
            nextAfterRemovedValue = null;
        }

        // don't call back treeChanged() for current cursor
        boolean result = tree.isAllowingDuplicates() ? tree.delete(key, value, this) : tree.delete(key, null, this);

        assert result;
        wasDelete = true;

        // root may be changed by tree.delete, so reset cursor with new root
        reset(tree.getRoot());

        return true;
    }

    @Override
    public boolean isMutable() {
        return true;
    }

    @Override
    public void treeChanged() {
        final ByteIterable key = getKey();
        final ByteIterable value = getValue();

        reset(tree.getRoot());

        // move to current
        final boolean withDuplicates = tree.isAllowingDuplicates();
        if (!traverser.moveTo(key, withDuplicates ? value : null)) {
            wasDelete = true;
            // null means current was removed
            // try to move to next key/value
            if (withDuplicates) {
                if (!(traverser.moveToRange(key, value) || traverser.moveToRange(key, null))) {
                    // null means key/value was removed, move to next key
                    return;
                }
            } else if (!traverser.moveToRange(key, null)) {
                return;
            }
            nextAfterRemovedKey = traverser.getKey();
            nextAfterRemovedValue = traverser.getValue();
        } else {
            inited = true;
        }
    }

    @Override
    public void close() {
        tree.cursorClosed(this);
    }

    public static void notifyCursors(ITreeMutable tree) {
        final Iterable<ITreeCursorMutable> openCursors = tree.getOpenCursors();
        if (openCursors != null) {
            for (ITreeCursorMutable cursor : openCursors) {
                cursor.treeChanged();
            }
        }
    }

    @SuppressWarnings({"ObjectEquality"})
    public static void notifyCursors(ITreeMutable tree, ITreeCursorMutable cursorToSkip) {
        final Iterable<ITreeCursorMutable> openCursors = tree.getOpenCursors();
        if (openCursors != null) {
            for (ITreeCursorMutable cursor : openCursors) {
                if (cursor != cursorToSkip) {
                    cursor.treeChanged();
                }
            }
        }
    }
}
