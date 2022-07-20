/*
 * *
 *  * Copyright 2010 - 2022 JetBrains s.r.o.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * https://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package jetbrains.exodus.log;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

public final class MutablePage {
    @Nullable
    MutablePage previousPage;
    @NotNull
    ByteBuffer bytes;
    final long pageAddress;
    int flushedCount;
    int committedCount;
    int writtenCount;

    MutablePage(@Nullable final MutablePage previousPage, @NotNull final ByteBuffer page,
                final long pageAddress, final int count) {
        this.previousPage = previousPage;
        this.bytes = page;
        this.pageAddress = pageAddress;
        flushedCount = committedCount = writtenCount = count;
    }

    public @NotNull ByteBuffer getBytes() {
        return bytes;
    }

    public int getCount() {
        return writtenCount;
    }

    void setCounts(final int count) {
        flushedCount = committedCount = writtenCount = count;
    }
}

