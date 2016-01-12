/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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
import jetbrains.exodus.tree.IExpirationChecker;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

public class BTreeReclaimTraverser extends BTreeTraverser {
    @NotNull
    protected final BTreeMutable mainTree;
    @NotNull
    protected final IExpirationChecker expirationChecker;
    protected boolean wasReclaim;
    @NotNull
    protected final List<RandomAccessLoggable> dupLeafsLo = new ArrayList<>();
    @NotNull
    protected final List<RandomAccessLoggable> dupLeafsHi = new ArrayList<>();

    public BTreeReclaimTraverser(@NotNull BTreeMutable mainTree, @NotNull IExpirationChecker expirationChecker) {
        super(mainTree.getRoot());
        this.mainTree = mainTree;
        this.expirationChecker = expirationChecker;
    }

    protected void setPage(BasePage node) {
        currentNode = node;
    }

    protected void pushChild(int index) {
        node = pushChild(new TreePos(currentNode, index), currentNode.getChild(index), 0);
    }

    protected void popAndMutate() {
        final BasePage node = currentNode;
        moveUp();
        if (node.isMutable()) {
            final BasePageMutable nodeMutable = currentNode.getMutableCopy(mainTree);
            nodeMutable.setMutableChild(currentPos, (BasePageMutable) node);
            currentNode = nodeMutable;
        }
    }

}
