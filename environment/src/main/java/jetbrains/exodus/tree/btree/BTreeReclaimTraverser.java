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

import jetbrains.exodus.log.RandomAccessLoggable;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

class BTreeReclaimTraverser extends BTreeTraverser {
    @NotNull
    protected final BTreeMutable mainTree;
    boolean wasReclaim;
    @NotNull
    final List<RandomAccessLoggable> dupLeafsLo = new ArrayList<>();
    @NotNull
    final List<RandomAccessLoggable> dupLeafsHi = new ArrayList<>();

    BTreeReclaimTraverser(@NotNull BTreeMutable mainTree) {
        super(mainTree.getRoot());
        this.mainTree = mainTree;
    }

    protected void setPage(BasePage node) {
        currentNode = node;
    }

    void pushChild(int index) {
        node = pushChild(new TreePos(currentNode, index), currentNode.getChild(index), 0);
    }

    void popAndMutate() {
        final BasePage node = currentNode;
        moveUp();
        if (node.isMutable()) {
            final BasePageMutable nodeMutable = currentNode.getMutableCopy(mainTree);
            nodeMutable.setMutableChild(currentPos, (BasePageMutable) node);
            currentNode = nodeMutable;
        }
    }

}
