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

import jetbrains.exodus.env.Cursor;
import jetbrains.exodus.tree.ITree;
import jetbrains.exodus.tree.ITreeMutable;
import jetbrains.exodus.tree.TreeCursorNoDuplicatesTest;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class BTreeCursorNoDuplicatesTest extends TreeCursorNoDuplicatesTest {

    @Override
    protected ITreeMutable createMutableTree(final boolean hasDuplicates, final int structureId) {
        return new BTreeEmpty(log, false, structureId).getMutableCopy();
    }

    @Override
    protected ITree openTree(long address, boolean hasDuplicates) {
        return new BTree(log, address, hasDuplicates, 1);
    }

    @Test
    public void testGetNextDup() throws IOException {
        final TreeAwareRunnable genNextDup = new TreeAwareRunnable(getTreeMutable()) {
            @Override
            public void run() {
                Cursor c = _t.openCursor();

                for (int i = 0; i < 1000; i++) {
                    assertTrue(c.getNext());
                    assertFalse(c.getNextDup());
                }

            }
        };

        genNextDup.run();
        long a = getTreeMutable().save();
        genNextDup.run();
        reopen();
        genNextDup.setTree(openTree(a, true));
        genNextDup.run();
    }

}
