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

import jetbrains.exodus.log.Loggable;
import org.jetbrains.annotations.NotNull;

class ChildReference extends ChildReferenceBase {

    long suffixAddress;

    ChildReference(final byte firstByte, final long suffixAddress) {
        super(firstByte);
        this.suffixAddress = suffixAddress;
    }

    ChildReference(byte firstByte) {
        super(firstByte);
        suffixAddress = Loggable.NULL_ADDRESS;
    }

    @Override
    boolean isMutable() {
        return false;
    }

    @Override
    @NotNull
    NodeBase getNode(@NotNull final PatriciaTreeBase tree) {
        return tree.loadNode(suffixAddress);
    }
}
