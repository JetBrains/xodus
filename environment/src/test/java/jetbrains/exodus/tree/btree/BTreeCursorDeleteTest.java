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

import jetbrains.exodus.bindings.IntegerBinding;
import jetbrains.exodus.env.Cursor;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class BTreeCursorDeleteTest extends BTreeTestBase {

    @Test
    public void testDeleteCursorNoDuplicates1() throws IOException {
        tm = createMutableTree(false, 1);
        getTreeMutable().put(kv(1, "1"));

        Cursor c = tm.openCursor();

        assertTrue(c.getNext());
        assertTrue(c.deleteCurrent());
        assertFalse(c.deleteCurrent());
        assertFalse(c.getNext());
    }

    @Test
    public void testDeleteCursorNoDuplicates2() throws IOException {
        tm = createEmptyTreeForCursor(1).getMutableCopy();

        for (int i = 0; i < 8; i++) {
            getTreeMutable().put(kv(i, "v" + i));
        }

        Cursor c = tm.openCursor();
        assertTrue(c.getNext());
        assertEquals(key(0), c.getKey());
        assertTrue(c.deleteCurrent());
        assertFalse(c.deleteCurrent());

        assertTrue(c.getNext());
        assertEquals(key(1), c.getKey());

        assertEquals(value("v7"), c.getSearchKey(key(7)));
        assertTrue(c.deleteCurrent());
        assertFalse(c.deleteCurrent());
        assertFalse(c.getNext());
    }

    @Test
    public void testDeleteCursorDuplicates1() throws IOException {
        tm = createMutableTree(true, 1);
        getTreeMutable().put(kv(1, "11"));
        getTreeMutable().put(kv(1, "12"));

        assertTrue(getTreeMutable().getRoot() instanceof BottomPageMutable);
        assertEquals(1, getTreeMutable().getRoot().getSize());
        assertTrue(getTreeMutable().getRoot().getKey(0) instanceof LeafNodeDupMutable);
        assertEquals(2, getTreeMutable().getRoot().getKey(0).getDupCount());

        Cursor c = getTreeMutable().openCursor();

        assertTrue(c.getNext());
        assertTrue(c.deleteCurrent());
        assertFalse(c.deleteCurrent());

        assertTrue(getTreeMutable().getRoot() instanceof BottomPageMutable);
        assertEquals(1, getTreeMutable().getRoot().getSize());
        assertTrue(getTreeMutable().getRoot().getKey(0) instanceof LeafNodeMutable);

        assertTrue(c.getNext());
        assertTrue(c.deleteCurrent());
        assertFalse(c.deleteCurrent());
        assertFalse(c.getNext());
    }

    @Test
    public void testDeleteCursorDuplicates2() throws IOException {
        tm = createEmptyTreeForCursor(1).getMutableCopy();

        for (int i = 0; i < 8; i++) {
            getTreeMutable().put(kv(i, "v" + i));
            getTreeMutable().put(kv(i, "vv" + i));
        }

        Cursor c = getTreeMutable().openCursor();

        assertEquals(true, c.getSearchBoth(key(1), value("vv1")));
        assertTrue(c.deleteCurrent());
        assertFalse(c.deleteCurrent());
        assertTrue(c.getNext());

        assertEquals(true, c.getSearchBoth(key(7), value("vv7")));
        assertTrue(c.deleteCurrent());
        assertFalse(c.getNext());
    }

    @Test
    public void testDeleteCursorDuplicates3() throws IOException {
        tm = createMutableTree(true, 1).getMutableCopy();

        for (int i = 0; i < 32; ++i) {
            for (int j = 0; j < 32; ++j) {
                getTreeMutable().put(IntegerBinding.intToEntry(i), IntegerBinding.intToEntry(j));
            }
        }

        for (int i = 0; i < 32; ++i) {
            final Cursor cursor = getTreeMutable().openCursor();
            Assert.assertNotNull(cursor.getSearchKeyRange(IntegerBinding.intToEntry(i)));
            for (int j = 0; j < 31; ++j) {
                cursor.deleteCurrent();
                Assert.assertTrue(cursor.getNext());
            }
        }

    }

}
