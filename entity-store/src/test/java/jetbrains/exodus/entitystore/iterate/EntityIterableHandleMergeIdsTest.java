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
package jetbrains.exodus.entitystore.iterate;

import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static jetbrains.exodus.entitystore.iterate.EntityIterableHandleBase.mergeLinkIds;
import static org.junit.Assert.assertArrayEquals;

public class EntityIterableHandleMergeIdsTest extends TestCase {

    public void testNull() {
        assertNull(mergeLinkIds(null, null));
        int[] ids = {1};
        _(ids, null, ids);
    }

    public void testMono() {
        final int[] data = {4, 5, 7, 8};
        _(new int[]{1}, data, new int[]{1, 4, 5, 7, 8});
        _(new int[]{4}, data, data);
        _(new int[]{6}, data, new int[]{4, 5, 6, 7, 8});
        _(new int[]{8}, data, data);
        _(new int[]{9}, data, new int[]{4, 5, 7, 8, 9});
    }

    public void testDuo() {
        final int[] data = {4, 5, 7, 8};
        _(new int[]{1, 4}, data, new int[]{1, 4, 5, 7, 8});
        _(new int[]{4, 5}, data, data);
        _(new int[]{4, 7}, data, data);
        _(new int[]{4, 8}, data, data);
        _(new int[]{6, 7}, data, new int[]{4, 5, 6, 7, 8});
        _(new int[]{6, 8}, data, new int[]{4, 5, 6, 7, 8});
        _(new int[]{7, 9}, data, new int[]{4, 5, 7, 8, 9});
        _(new int[]{8, 9}, data, new int[]{4, 5, 7, 8, 9});
    }

    public void testTri() {
        final int[] data = {4, 5, 7, 8};
        _(new int[]{1, 5, 8}, data, new int[]{1, 4, 5, 7, 8});
        _(new int[]{4, 7, 8}, data, data);
        _(new int[]{6, 7, 8}, data, new int[]{4, 5, 6, 7, 8});
        _(new int[]{4, 6, 8}, data, new int[]{4, 5, 6, 7, 8});
        _(new int[]{5, 7, 9}, data, new int[]{4, 5, 7, 8, 9});
        _(new int[]{4, 8, 9}, data, new int[]{4, 5, 7, 8, 9});
    }

    private static void _(@Nullable int[] left, @Nullable int[] right, @NotNull int[] expected) {
        assertArrayEquals(expected, mergeLinkIds(left, right));
        assertArrayEquals(expected, mergeLinkIds(right, left));
    }
}
