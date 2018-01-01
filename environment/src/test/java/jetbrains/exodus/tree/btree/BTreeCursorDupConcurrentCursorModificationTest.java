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
import jetbrains.exodus.tree.ITreeCursor;
import org.jetbrains.annotations.NotNull;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class BTreeCursorDupConcurrentCursorModificationTest extends BTreeCursorDupConcurrentModificationTest {
    @Override
    protected void deleteImpl(@NotNull ByteIterable key) {
        final ITreeCursor cursor = tm.openCursor();
        assertNotNull(cursor.getSearchKey(key));
        assertTrue(cursor.deleteCurrent());
    }

    @Override
    protected void deleteImpl(@NotNull final ByteIterable key, @NotNull final ByteIterable value) {
        final ITreeCursor cursor = tm.openCursor();
        assertTrue(cursor.getSearchBoth(key, value));
        assertTrue(cursor.deleteCurrent());
    }
}
