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

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.env.Cursor;
import org.jetbrains.annotations.NotNull;

final class SingleKeyCursorCounter {

    private final long count;

    SingleKeyCursorCounter(@NotNull final Cursor cursor, @NotNull final ByteIterable keyEntry) {
        try {
            final boolean success = cursor.getSearchKey(keyEntry) != null;
            count = success ? cursor.count() : 0;
        } finally {
            cursor.close();
        }
    }

    public long getCount() {
        return count;
    }
}
