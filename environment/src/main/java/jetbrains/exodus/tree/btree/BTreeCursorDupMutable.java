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
import jetbrains.exodus.tree.ITreeMutable;
import jetbrains.exodus.tree.MutableTreeRoot;
import jetbrains.exodus.tree.TreeCursorMutable;
import org.jetbrains.annotations.NotNull;

public class BTreeCursorDupMutable extends TreeCursorMutable {

    @NotNull
    protected final BTreeTraverserDup traverser; // hack to avoid casts

    public BTreeCursorDupMutable(ITreeMutable tree, BTreeTraverserDup traverser) {
        super(tree, traverser);
        this.traverser = traverser;
    }

    @Override
    public boolean getNextDup() {
        moveIfNecessary();
        // move to next dup if in -1 position or dupCursor has next element
        if (traverser.node == ILeafNode.EMPTY) {
            if (wasDelete) {
                final BasePage root = traverser.currentNode; // traverser was reset to root after delete
                final ByteIterable key = nextAfterRemovedKey;
                final ByteIterable value = nextAfterRemovedValue;
                if (getNext()) {
                    if (traverser.inDupTree) {
                        return true;
                    }
                    // not really a dup, rollback
                    reset((MutableTreeRoot) root); // also restores state
                    wasDelete = true;
                    nextAfterRemovedKey = key;
                    nextAfterRemovedValue = value;
                }
                return false;
            }
            return getNext();
        } else {
            return hasNext() && traverser.inDupTree && getNext() && traverser.inDupTree;
        }
    }

    @Override
    public boolean getNextNoDup() {
        moveIfNecessary();
        if (wasDelete) {
            if (getNext()) {
                /* we managed to re-navigate to key which is next
                   after removed, check if it is a duplicate */
                if (!traverser.inDupTree) {
                    return true;
                }
            } else {
                return false;
            }
        }
        if (traverser.inDupTree) {
            traverser.popUntilDupRight();
            canGoDown = false;
        }
        return getNext();
    }

    @Override
    public boolean getPrevDup() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getPrevNoDup() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int count() {
        moveIfNecessary();
        return traverser.inDupTree ? (int) traverser.currentNode.getTree().size : super.count();
    }

}
