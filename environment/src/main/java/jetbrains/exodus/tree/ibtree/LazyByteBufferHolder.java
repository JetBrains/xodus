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

import jetbrains.exodus.log.Log;
import jetbrains.exodus.tree.ExpiredLoggableCollection;

import java.nio.ByteBuffer;

final class LazyByteBufferHolder implements ByteBufferHolder {
    private final Log log;
    private final long address;

    private ByteBuffer buffer;
    private int loggableSize;

    LazyByteBufferHolder(Log log, long address) {
        this.log = log;
        this.address = address;
    }

    @Override
    public ByteBuffer getByteBuffer() {
        if (buffer != null) {
            return buffer;
        }

        var loggable = log.read(address);
        loggableSize = loggable.length();

        var data = loggable.getData();

        buffer = data.getByteBuffer();
        return buffer;
    }

    @Override
    public void addExpiredLoggable(ExpiredLoggableCollection expiredLoggableCollection) {
        if (loggableSize == 0) {
            var loggable = log.read(address);
            loggableSize = loggable.length();
        }

        expiredLoggableCollection.add(address, loggableSize);
    }

    @Override
    public long getAddress() {
        return address;
    }

    @Override
    public int embeddedSize() {
        return 0;
    }
}
