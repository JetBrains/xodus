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
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BTreeSpecificCursorDuplicatesTest extends BTreeTestBase {

    @Test
    public void testGetSearchKeyRange3() throws IOException {
        tm = createEmptyTreeForCursor(1).getMutableCopy();

        getTreeMutable().put(kv(2, "v1"));
        getTreeMutable().put(kv(2, "v2"));
        getTreeMutable().put(kv(2, "v3"));
        getTreeMutable().put(kv(3, "v5"));
        getTreeMutable().put(kv(3, "v6"));
        getTreeMutable().put(kv(3, "v7"));

        assertMatches(getTreeMutable(), BP(2));

        final TreeAwareRunnable getDups = new TreeAwareRunnable(getTreeMutable()) {
            @Override
            public void run() {
                Cursor c = _t.openCursor();
                c.getSearchKey(key(2));
                assertTrue(c.getNextDup());
                assertTrue(c.getNextDup());
                assertFalse(c.getNextDup());
            }
        };

        getDups.run();
        long a = getTreeMutable().save();
        getDups.run();
        reopen();
        getDups.setTree(openTree(a, true));
        getDups.run();
    }
}
