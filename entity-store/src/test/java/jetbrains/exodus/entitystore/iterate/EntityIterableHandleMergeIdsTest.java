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
package jetbrains.exodus.entitystore.iterate;

import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;

import static jetbrains.exodus.entitystore.iterate.EntityIterableHandleBase.mergeFieldIds;
import static org.junit.Assert.assertArrayEquals;

public class EntityIterableHandleMergeIdsTest extends TestCase {

    public void testEmpty() {
        assertNotNull(mergeFieldIds(new int[0], new int[0]));
        int[] ids = {1};
        checkArrays(ids, new int[0], ids);
    }

    public void testMono() {
        final int[] data = {4, 5, 7, 8};
        checkArrays(new int[]{1}, data, new int[]{1, 4, 5, 7, 8});
        checkArrays(new int[]{4}, data, data);
        checkArrays(new int[]{6}, data, new int[]{4, 5, 6, 7, 8});
        checkArrays(new int[]{8}, data, data);
        checkArrays(new int[]{9}, data, new int[]{4, 5, 7, 8, 9});
    }

    public void testDuo() {
        final int[] data = {4, 5, 7, 8};
        checkArrays(new int[]{1, 4}, data, new int[]{1, 4, 5, 7, 8});
        checkArrays(new int[]{4, 5}, data, data);
        checkArrays(new int[]{4, 7}, data, data);
        checkArrays(new int[]{4, 8}, data, data);
        checkArrays(new int[]{6, 7}, data, new int[]{4, 5, 6, 7, 8});
        checkArrays(new int[]{6, 8}, data, new int[]{4, 5, 6, 7, 8});
        checkArrays(new int[]{7, 9}, data, new int[]{4, 5, 7, 8, 9});
        checkArrays(new int[]{8, 9}, data, new int[]{4, 5, 7, 8, 9});
    }

    public void testTri() {
        final int[] data = {4, 5, 7, 8};
        checkArrays(new int[]{1, 5, 8}, data, new int[]{1, 4, 5, 7, 8});
        checkArrays(new int[]{4, 7, 8}, data, data);
        checkArrays(new int[]{6, 7, 8}, data, new int[]{4, 5, 6, 7, 8});
        checkArrays(new int[]{4, 6, 8}, data, new int[]{4, 5, 6, 7, 8});
        checkArrays(new int[]{5, 7, 9}, data, new int[]{4, 5, 7, 8, 9});
        checkArrays(new int[]{4, 8, 9}, data, new int[]{4, 5, 7, 8, 9});
    }

    private static void checkArrays(@NotNull int[] left, @NotNull int[] right, @NotNull int[] expected) {
        assertArrayEquals(expected, mergeFieldIds(left, right));
        assertArrayEquals(expected, mergeFieldIds(right, left));
    }
}
