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

package jetbrains.exodus.tree.ibtree;

import jetbrains.exodus.tree.ExpiredLoggableCollection;

import java.nio.ByteBuffer;

final class LoadedByteBufferHolder implements ByteBufferHolder {
    private final ByteBuffer buffer;
    private final int maxSize;

    public LoadedByteBufferHolder(ByteBuffer buffer, int maxSize) {
        this.buffer = buffer;
        this.maxSize = maxSize;
    }

    @Override
    public ByteBuffer getByteBuffer() {
        return buffer;
    }

    @Override
    public void addExpiredLoggable(ExpiredLoggableCollection expiredLoggableCollection) {
        //do nothing
    }

    @Override
    public long getAddress() {
        return 0;
    }

    @Override
    public int embeddedSize() {
        final int size = buffer.limit();
        if (size <= maxSize) {
            return size;
        }

        return 0;
    }
}
