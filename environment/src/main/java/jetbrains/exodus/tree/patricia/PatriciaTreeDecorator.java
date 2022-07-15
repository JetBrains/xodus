/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.tree.patricia;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.log.DataIterator;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.tree.INode;
import jetbrains.exodus.tree.ITree;
import org.jetbrains.annotations.NotNull;

import java.io.PrintStream;

abstract class PatriciaTreeDecorator implements ITree {

    @NotNull
    protected final ITree treeNoDuplicates;

    protected PatriciaTreeDecorator(@NotNull final ITree treeNoDuplicates) {
        this.treeNoDuplicates = treeNoDuplicates;
    }

    @NotNull
    @Override
    public Log getLog() {
        return treeNoDuplicates.getLog();
    }

    @NotNull
    @Override
    public DataIterator getDataIterator(long address) {
        return treeNoDuplicates.getDataIterator(address);
    }

    @Override
    public long getRootAddress() {
        return treeNoDuplicates.getRootAddress();
    }

    @Override
    public int getStructureId() {
        return treeNoDuplicates.getStructureId();
    }

    @Override
    public boolean hasKey(@NotNull final ByteIterable key) {
        return get(key) != null;
    }

    @Override
    public boolean isEmpty() {
        return treeNoDuplicates.isEmpty();
    }

    @Override
    public long getSize() {
        return treeNoDuplicates.getSize();
    }
}
