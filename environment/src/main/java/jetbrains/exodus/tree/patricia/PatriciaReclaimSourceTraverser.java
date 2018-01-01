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

import jetbrains.exodus.tree.INode;
import org.jetbrains.annotations.NotNull;

final class PatriciaReclaimSourceTraverser extends PatriciaTraverser {

    private final long minAddress;

    PatriciaReclaimSourceTraverser(@NotNull final PatriciaTreeBase tree,
                                   @NotNull final NodeBase currentNode,
                                   final long minAddress) {
        super(tree, currentNode);
        this.minAddress = minAddress;
        init(true);
    }

    @Override
    @NotNull
    public INode moveRight() {
        INode result;
        do {
            result = super.moveRight();
        } while (isValidPos() && !isAddressReclaimable(currentChild.suffixAddress));
        return result;
    }

    void moveToNextReclaimable() {
        while (isValidPos() && !isAddressReclaimable(currentChild.suffixAddress)) {
            super.moveRight();
        }
    }

    boolean isAddressReclaimable(final long address) {
        return minAddress <= address;
    }
}
