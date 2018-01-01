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
package jetbrains.exodus.tree.btree;

import jetbrains.exodus.tree.ITree;
import jetbrains.exodus.tree.ITreeMutable;
import jetbrains.exodus.tree.TreeDeleteTest;

public class BTreeDeleteCommonTest extends TreeDeleteTest {

    private final BTreeBalancePolicy policy = new BTreeBalancePolicy(2);

    @Override
    protected ITreeMutable createMutableTree(final boolean hasDuplicates, final int structureId) {
        return new BTreeEmpty(log, policy, hasDuplicates, structureId).getMutableCopy();
    }

    @Override
    protected ITree openTree(long address, boolean hasDuplicates) {
        return new BTree(log, policy, address, hasDuplicates, 1);
    }
}
