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

import jetbrains.exodus.ByteBufferByteIterable;
import jetbrains.exodus.ByteIterable;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class ByteBufferLoggable implements Loggable {
    private final long address;
    private final byte type;
    private final int length;
    private final int dataLength;
    private final int structureId;
    private final ByteBuffer buffer;

    public ByteBufferLoggable(final long address, final byte type, final int length,
                              final int dataLength, final int structureId, final ByteBuffer buffer) {
        this.address = address;
        this.type = type;
        this.length = length;
        this.dataLength = dataLength;
        this.structureId = structureId;
        this.buffer = buffer;
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
        return length;
    }

    @Override
    public @NotNull ByteIterable getData() {
        return new ByteBufferByteIterable(buffer);
    }

    public @NotNull ByteBuffer getBuffer() {
        return buffer;
    }

    @Override
    public int getDataLength() {
        return dataLength;
    }

    @Override
    public int getStructureId() {
        return structureId;
    }
}
