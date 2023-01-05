/**
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

import org.jetbrains.annotations.NotNull;

public final class MultiPageLoggable implements RandomAccessLoggable {

    private final byte type;
    @NotNull
    private final MultiPageByteIterableWithAddress data;
    private final int structureId;
    private final int dataLength;
    private final long address;
    private final Log log;

    public MultiPageLoggable(final long address,
                             final byte type,
                             @NotNull final MultiPageByteIterableWithAddress data,
                             final int dataLength,
                             final int structureId,
                             final Log log) {
        this.address = address;
        this.type = type;
        this.data = data;
        this.structureId = structureId;
        this.dataLength = dataLength;
        this.log = log;
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
        assert log != null;
        return dataLength + CompressedUnsignedLongByteIterable.getCompressedSize(structureId) +
                CompressedUnsignedLongByteIterable.getCompressedSize(dataLength) + 1;
    }

    @Override
    public long end() {
        assert log != null;
        return log.adjustLoggableAddress(data.getDataAddress(), dataLength);
    }

    @NotNull
    @Override
    public MultiPageByteIterableWithAddress getData() {
        return data;
    }

    public int getDataLength() {
        return dataLength;
    }

    @Override
    public int getStructureId() {
        return structureId;
    }

    @Override
    public boolean isDataInsideSinglePage() {
        return false;
    }
}
