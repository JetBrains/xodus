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

final class EmptyRandomAccessLoggable implements RandomAccessLoggable {
    private final long address;
    private final int size;
    private final byte type;

    EmptyRandomAccessLoggable(long address, int size, byte type) {
        this.address = address;
        this.size = size;
        this.type = type;
    }

    @Override
    public long getAddress() {
        return address;
    }

    @Override
    public byte getType() {
        return type;
    }

    @Override
    public int length() {
        return size;
    }

    @Override
    public @NotNull ByteIterableWithAddress getData() {
        return ByteIterableWithAddress.EMPTY;
    }

    @Override
    public int getDataLength() {
        return 0;
    }

    @Override
    public int getStructureId() {
        return Loggable.NO_STRUCTURE_ID;
    }
}
