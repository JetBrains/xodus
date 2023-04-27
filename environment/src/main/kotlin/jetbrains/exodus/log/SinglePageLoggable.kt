/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.log;

import jetbrains.exodus.ByteIterable;
import org.jetbrains.annotations.NotNull;

class SinglePageLoggable implements RandomAccessLoggable {
    public static final SinglePageLoggable NULL_PROTOTYPE = new SinglePageLoggable(Loggable.NULL_ADDRESS,
            Loggable.NULL_ADDRESS, (byte) 0, Loggable.NO_STRUCTURE_ID, Loggable.NULL_ADDRESS, ByteIterable.EMPTY_BYTES,
            0, 0);

    private final long address;
    private final long end;
    private final int structureId;
    private final byte type;

    private int length = -1;

    private final ArrayByteIterableWithAddress data;

    SinglePageLoggable(final long address,
                       final long end,
                       final byte type,
                       final int structureId,
                       final long dataAddress,
                       final byte @NotNull [] bytes,
                       final int start,
                       final int dataLength) {
        this.data = new ArrayByteIterableWithAddress(dataAddress, bytes, start, dataLength);
        this.structureId = structureId;
        this.type = type;
        this.address = address;
        this.end = end;
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
        if (length >= 0) {
            return length;
        }

        var dataLength = getDataLength();
        length = dataLength + CompressedUnsignedLongByteIterable.getCompressedSize(structureId) +
                CompressedUnsignedLongByteIterable.getCompressedSize(dataLength) + 1;

        return length;
    }

    @Override
    public long end() {
        return end;
    }

    @NotNull
    @Override
    public ArrayByteIterableWithAddress getData() {
        return data;
    }

    @Override
    public int getDataLength() {
        return data.getLength();
    }

    @Override
    public int getStructureId() {
        return structureId;
    }

    @Override
    public boolean isDataInsideSinglePage() {
        return true;
    }
}
