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
package jetbrains.exodus.log;

import org.jetbrains.annotations.NotNull;

class RandomAccessLoggableAndArrayByteIterable extends ArrayByteIterableWithAddress implements RandomAccessLoggable {

    private final int structureId;
    private final byte type;
    private final byte headerLength;

    RandomAccessLoggableAndArrayByteIterable(final long address,
                                             final byte type,
                                             final int structureId,
                                             final long dataAddress,
                                             @NotNull final byte[] bytes,
                                             final int start,
                                             final int dataLength) {
        super(dataAddress, bytes, start, dataLength);
        this.structureId = structureId;
        this.type = type;
        headerLength = (byte) (dataAddress - address);
    }

    @Override
    public long getAddress() {
        return getDataAddress() - headerLength;
    }

    @Override
    public byte getType() {
        return type;
    }

    @Override
    public int length() {
        return headerLength + getDataLength();
    }

    @NotNull
    @Override
    public ByteIterableWithAddress getData() {
        return this;
    }

    @Override
    public int getDataLength() {
        return getLength();
    }

    @Override
    public int getStructureId() {
        return structureId;
    }
}
