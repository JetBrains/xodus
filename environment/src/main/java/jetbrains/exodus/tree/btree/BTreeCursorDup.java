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

import jetbrains.exodus.tree.TreeCursor;
import org.jetbrains.annotations.NotNull;

/**
 * BTree iterator with duplicates support
 */
class BTreeCursorDup extends TreeCursor {

    @NotNull
    protected final BTreeTraverserDup traverser; // hack to avoid casts

    BTreeCursorDup(BTreeTraverserDup traverser) {
        super(traverser);
        this.traverser = traverser;
    }

    @Override
    public boolean getNextDup() {
        // move to next dup if in -1 position or dupCursor has next element
        return hasNext() && traverser.inDupTree && getNext() && traverser.inDupTree;
    }

    @Override
    public boolean getNextNoDup() {
        if (traverser.inDupTree) {
            traverser.popUntilDupRight();
            canGoDown = false;
        }
        return getNext();
    }

    @Override
    public boolean getPrevDup() {
        // move to next dup if in -1 position or dupCursor has next element
        return hasPrev() && traverser.inDupTree && getPrev() && traverser.inDupTree;
    }

    @Override
    public boolean getPrevNoDup() {
        traverser.popUntilDupLeft(); // ignore duplicates
        return getPrev();
    }

    @Override
    public int count() {
        return traverser.inDupTree ? (int) traverser.currentNode.getTree().size : super.count();
    }

}
