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
package jetbrains.exodus.benchmark.tree;

import jetbrains.exodus.tree.ITree;
import jetbrains.exodus.tree.ITreeMutable;
import jetbrains.exodus.tree.btree.BTree;
import jetbrains.exodus.tree.btree.BTreeBalancePolicy;
import jetbrains.exodus.tree.btree.BTreeEmpty;

public class BTreeTokyoCabinetLikeBenchmarkTest extends TokyoCabinetLikeBenchmarkTestBase {

    private static final int pageSize = 256;

    @Override
    protected ITreeMutable createMutableTree(final boolean hasDuplicates, final int structureId) {
        return new BTreeEmpty(log, getBTreeBalancePolicy(), false, 1).getMutableCopy();
    }

    @Override
    protected ITree openTree(long address, boolean hasDuplicates) {
        return new BTree(log, getBTreeBalancePolicy(), address, false, 1);
    }

    private static BTreeBalancePolicy getBTreeBalancePolicy() {
        return new BTreeBalancePolicy(pageSize);
    }
}
