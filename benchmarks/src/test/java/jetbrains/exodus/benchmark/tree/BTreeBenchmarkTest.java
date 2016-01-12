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
import jetbrains.exodus.tree.btree.BTreeEmpty;
import org.junit.Test;

import java.io.IOException;

/**
 */
public class BTreeBenchmarkTest extends TreeBenchmarkTestBase {

    @Test
    public void testAddLastNoDuplicates() throws IOException {
        final int s = 10000;

        time("Put: ", new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < s; i++) {
                    if (i % 1000 == 0) System.out.println(i);
                    tm.put(kv("key" + i, "value" + i));
                }
            }
        }, s);

        time("Save: ", new Runnable() {
            @Override
            public void run() {
                tm.save();
            }
        }, s);
    }

    @Override
    protected ITreeMutable createMutableTree(final boolean hasDuplicates, final int structureId) {
        return new BTreeEmpty(log, hasDuplicates, structureId).getMutableCopy();
    }

    @Override
    protected ITree openTree(long address, boolean hasDuplicates) {
        return null;
    }
}
