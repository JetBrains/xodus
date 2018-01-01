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

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.log.Loggable;
import jetbrains.exodus.util.LightOutputStream;
import org.jetbrains.annotations.NotNull;

class MutableNodeSaveContext {

    @NotNull
    final ByteIterable preliminaryRootData;
    long startAddress;
    private final LightOutputStream nodeStream;

    MutableNodeSaveContext(@NotNull final ByteIterable preliminaryRootData) {
        this.preliminaryRootData = preliminaryRootData;
        startAddress = Loggable.NULL_ADDRESS;
        nodeStream = new LightOutputStream(16);
    }

    public LightOutputStream newNodeStream() {
        nodeStream.clear();
        return nodeStream;
    }
}
