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

import org.jetbrains.annotations.NotNull;

final class PatriciaReclaimActualTraverser extends PatriciaTraverser {

    @NotNull
    final PatriciaTreeMutable mainTree;
    boolean wasReclaim;

    PatriciaReclaimActualTraverser(@NotNull final PatriciaTreeMutable mainTree) {
        super(mainTree, mainTree.getRoot());
        this.mainTree = mainTree;
        init(true);
    }

    void popAndMutate() {
        --top;
        final NodeChildrenIterator topItr = stack[top];
        final NodeBase parentNode = topItr.getParentNode();
        if (currentNode.isMutable()) {
            final int pos = topItr.getIndex();
            final MutableNode parentNodeMutable = parentNode.getMutableCopy(mainTree);
            parentNodeMutable.setChild(pos, (MutableNode) currentNode);
            currentNode = parentNodeMutable;
            currentChild = parentNodeMutable.getRef(pos);
            // mutate iterator to boost performance
            currentIterator = parentNode.isMutable() ? topItr : parentNodeMutable.getChildren(pos);
            // currentIterator = topItr;
        } else {
            currentNode = parentNode;
            currentIterator = topItr;
            currentChild = topItr.getNode();
        }
        stack[top] = null; // help gc
    }
}
