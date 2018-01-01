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

import jetbrains.exodus.tree.MutableTreeRoot;
import org.jetbrains.annotations.NotNull;

final class MutableRoot extends MutableNode implements MutableTreeRoot {

    final long sourceAddress;

    MutableRoot(@NotNull final ImmutableNode origin) {
        super(origin);
        sourceAddress = origin.getAddress();
    }

    MutableRoot(@NotNull final MutableNode node, long sourceAddress) {
        super(node.keySequence, node.value, node.children);
        this.sourceAddress = sourceAddress;
    }

    @Override
    protected boolean isRoot() {
        return true;
    }
}
