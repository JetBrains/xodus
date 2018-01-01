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

final class ChildReferenceTransient extends ChildReferenceBase {

    @NotNull
    NodeBase child;

    ChildReferenceTransient(final byte firstByte, @NotNull final NodeBase child) {
        super(firstByte);
        this.child = child;
    }

    @Override
    boolean isMutable() {
        return child.isMutable();
    }

    @Override
    NodeBase getNode(@NotNull final PatriciaTreeBase tree) {
        return child;
    }

    MutableNode mutate(@NotNull final PatriciaTreeMutable tree) {
        return (MutableNode) (child = child.getMutableCopy(tree));
    }
}
