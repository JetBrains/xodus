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

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.env.Cursor;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class BTreeCursorDupConcurrentModificationTest extends BTreeTestBase {

    @Before
    public void prepareTree() {
        tm = new BTreeEmpty(log, new BTreeBalancePolicy(5), true, 1).getMutableCopy();

        tm.put(key(1), value("v10"));
        tm.put(key(2), value("v20"));
        tm.put(key(3), value("v30"));
        tm.put(key(4), value("v40"));
        tm.put(key(5), value("v50"));
        tm.put(key(5), value("v51"));
        tm.put(key(5), value("v52"));
        tm.put(key(6), value("v60"));
        tm.put(key(7), value("v70"));
    }

    @Test
    public void testConcurrentDeleteBefore1() {
        Cursor c = tm.openCursor();
        assertTrue(c.getSearchBoth(key(5), value("v51")));
        deleteImpl(key(1));
        assertTrue(c.getNext());
        assertEquals(key(5), c.getKey());
        assertEquals(value("v52"), c.getValue());

        assertTrue(c.getNext());
        assertEquals(key(6), c.getKey());
    }

    @Test
    public void testConcurrentDeleteAfter() {
        Cursor c = tm.openCursor();
        assertTrue(c.getSearchBoth(key(5), value("v51")));
        deleteImpl(key(6));
        assertTrue(c.getNext());
        assertEquals(key(5), c.getKey());
        assertEquals(value("v52"), c.getValue());

        assertTrue(c.getNext());
        assertEquals(key(7), c.getKey());
    }

    @Test
    public void testConcurrentDeleteCurrent() {
        Cursor c = tm.openCursor();
        final ByteIterable value = value("v51");
        assertTrue(c.getSearchBoth(key(5), value));
        deleteImpl(key(5), value);
        assertTrue(c.getNext());
        assertEquals(key(5), c.getKey());
        assertEquals(value("v52"), c.getValue());
    }

    protected void deleteImpl(@NotNull final ByteIterable key) {
        assertTrue(getTreeMutable().delete(key));
    }

    protected void deleteImpl(@NotNull final ByteIterable key, @NotNull final ByteIterable value) {
        assertTrue(getTreeMutable().delete(key, value));
    }

}
