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
import jetbrains.exodus.tree.patricia.PatriciaTree;
import jetbrains.exodus.tree.patricia.PatriciaTreeEmpty;
import jetbrains.exodus.tree.patricia.PatriciaTreeWithDuplicates;

public class PatriciaTokyoCabinetLikeBenchmarkTest extends TokyoCabinetLikeBenchmarkTestBase {

    @Override
    protected ITreeMutable createMutableTree(final boolean hasDuplicates, final int structureId) {
        return new PatriciaTreeEmpty(log, 1, hasDuplicates).getMutableCopy();
    }

    @Override
    protected ITree openTree(long address, boolean hasDuplicates) {
        return hasDuplicates ? new PatriciaTreeWithDuplicates(log, address, 1) : new PatriciaTree(log, address, 1);
    }
}
