/*
 * *
 *  * Copyright 2010 - 2022 JetBrains s.r.o.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * https://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package jetbrains.exodus.tree.ibtree;

import jetbrains.exodus.log.NullLoggable;
import jetbrains.exodus.tree.ITree;
import jetbrains.exodus.tree.ITreeMutable;
import jetbrains.exodus.tree.TreeBaseTest;

import java.util.function.Consumer;

public class BTreeTestBase extends TreeBaseTest {
    @Override
    protected ITreeMutable createMutableTree(boolean hasDuplicates, int structureId) {
        var immutableTree = new ImmutableBTree(log, structureId, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        tm = immutableTree.getMutableCopy();
        return tm;
    }

    @Override
    protected ITree openTree(long address, boolean hasDuplicates) {
        t = new ImmutableBTree(log, -1, log.getCachePageSize(), address);
        return t;
    }

    protected ITree openTree(long address, boolean hasDuplicates, int structureId) {
        t = new ImmutableBTree(log, structureId, log.getCachePageSize(), address);
        return t;
    }

    protected void checkTree(boolean hasDuplicates, Consumer<ITree> checker) {
        checker.accept(tm);

        long address = saveTree();

        checker.accept(tm);

        var structureId = tm.getStructureId();

        openTree(address, hasDuplicates, structureId);

        checker.accept(t);

        reopen();

        openTree(address, hasDuplicates, structureId);

        checker.accept(t);
    }
}
