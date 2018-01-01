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

import org.jetbrains.annotations.NotNull;

class BTreeTraverserDup extends BTreeTraverser {

    protected boolean inDupTree;

    BTreeTraverserDup(@NotNull BasePage currentNode) {
        super(currentNode);
    }

    @Override
    public boolean canMoveDown() {
        // TODO: simplify, optimize
        return currentPos < currentNode.size && (super.canMoveDown() || (currentPos >= 0 && currentNode.getKey(currentPos).isDup()));
    }

    @Override
    protected BasePage getChildForMoveDown() {
        if (currentNode.isBottom()) {
            final BaseLeafNode leaf = currentNode.getKey(currentPos);
            inDupTree = true;
            return leaf.getTree().getRoot();
        } else {
            return super.getChildForMoveDown();
        }
    }

    @Override
    public void moveUp() {
        super.moveUp();
        if (currentNode.isBottom()) {
            // we moved up and we are at bottom: this means we were in a dup tree
            inDupTree = false;
        }
    }

    @Override
    protected ILeafNode handleLeaf(BaseLeafNode leaf) {
        if (leaf.isDupLeaf()) {
            return new LeafNodeKV(leaf.getValue(), leaf.getKey());
        } else {
            return super.handleLeaf(leaf);
        }
    }

    @Override
    protected ILeafNode handleLeafR(BaseLeafNode leaf) {
        if (leaf.isDupLeaf()) {
            return new LeafNodeKV(leaf.getValue(), leaf.getKey());
        } else if (leaf.isDup()) {
            inDupTree = true;
            return pushChild(new TreePos(currentNode, currentPos), leaf.getTree().getRoot(), 0);
        } else {
            return super.handleLeaf(leaf);
        }
    }

    @Override
    protected ILeafNode handleLeafL(BaseLeafNode leaf) {
        if (leaf.isDupLeaf()) {
            return new LeafNodeKV(leaf.getValue(), leaf.getKey());
        } else if (leaf.isDup()) {
            inDupTree = true;
            BasePage root = leaf.getTree().getRoot();
            return pushChild(new TreePos(currentNode, currentPos), root, root.size - 1);
        } else {
            return super.handleLeaf(leaf);
        }
    }

    protected void popUntilDupRight() {
        int bottom = top;
        while (true) {
            --bottom;
            final TreePos current = stack[bottom];
            stack[bottom] = null; // gc
            if (current.node.isBottom()) {
                currentNode = current.node;
                currentPos = current.pos;
                top = bottom;
                inDupTree = false;
                return;
            }
        }
    }

    protected void popUntilDupLeft() {
        /*if (false) {
            final int bottom = 0;
            final TreePos current = stack[bottom];
            currentNode = current.node;
            currentPos = current.pos;
            top = bottom;
            while (top > bottom) {
                --top;
                stack[top] = null;
            }
        }*/
    }

    @Override
    protected boolean isDup() {
        return true;
    }
}
