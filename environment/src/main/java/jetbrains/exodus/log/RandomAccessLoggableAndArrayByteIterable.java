/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.log;

import org.jetbrains.annotations.NotNull;

class RandomAccessLoggableAndArrayByteIterable extends ArrayByteIterableWithAddress implements RandomAccessLoggable {

    private final long address;
    private final long end;
    private final int structureId;
    private final byte type;
    private final int length;
    private final boolean dataInsideSinglePage;

    RandomAccessLoggableAndArrayByteIterable(final long address,
                                             final long end,
                                             final byte type,
                                             final int structureId,
                                             final int length,
                                             final long dataAddress,
                                             final byte @NotNull [] bytes,
                                             final int start,
                                             final int dataLength,
                                             final boolean dataInsideSinglePage) {
        super(dataAddress, bytes, start, dataLength);
        this.structureId = structureId;
        this.type = type;
        this.address = address;
        this.end = end;
        this.length = length;
        this.dataInsideSinglePage = dataInsideSinglePage;
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
    public long end() {
        return end;
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

    @Override
    public boolean isDataInsideSinglePage() {
        return dataInsideSinglePage;
    }
}
