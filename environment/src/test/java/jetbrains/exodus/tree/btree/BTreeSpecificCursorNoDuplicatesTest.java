/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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

import static org.junit.Assert.assertEquals;

public class BTreeSpecificCursorNoDuplicatesTest extends BTreeTestBase {
    @Test
    public void testGetSearchKeyRange3() {
        tm = createEmptyTreeForCursor(1).getMutableCopy();

        getTreeMutable().put(kv(1, "v1"));
        getTreeMutable().put(kv(2, "v2"));
        getTreeMutable().put(kv(3, "v3"));
        getTreeMutable().put(kv(5, "v5"));
        getTreeMutable().put(kv(6, "v6"));
        getTreeMutable().put(kv(7, "v7"));

        assertMatches(getTreeMutable(), IP(BP(3), BP(3)));

        final TreeAwareRunnable getSearchKeyRange = new TreeAwareRunnable(getTreeMutable()) {
            @Override
            public void run() {
                Cursor c = _t.openCursor();

                assertEquals(value("v5"), c.getSearchKeyRange(key(4)));
                assertEquals(key(5), c.getKey());
            }
        };

        getSearchKeyRange.run();
        long a = saveTree();
        getSearchKeyRange.run();
        reopen();
        getSearchKeyRange.setTree(openTree(a, true));
        getSearchKeyRange.run();
    }
}
