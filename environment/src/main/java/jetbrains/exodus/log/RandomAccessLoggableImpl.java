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

public class RandomAccessLoggableImpl implements RandomAccessLoggable {

    private final byte type;
    @NotNull
    private final ByteIterableWithAddress data;
    private final int structureId;
    private final int dataLength;
    private final long address;
    private final boolean dataInsideSinglePage;
    private final Log log;
    private int length = -1;
    private long end = -1;

    public RandomAccessLoggableImpl(final long address,
                                    final long end,
                                    final int length,
                                    final byte type,
                                    @NotNull final ByteIterableWithAddress data,
                                    final int dataLength,
                                    final int structureId,
                                    final boolean dataInsideSinglePage) {
        this.address = address;
        this.end = end;
        this.type = type;
        this.length = length;
        this.data = data;
        this.structureId = structureId;
        this.dataLength = dataLength;
        this.dataInsideSinglePage = dataInsideSinglePage;
        this.log = null;
    }

    public RandomAccessLoggableImpl(final long address,
                                    final byte type,
                                    @NotNull final ByteIterableWithAddress data,
                                    final int dataLength,
                                    final int structureId,
                                    final boolean dataInsideSinglePage,
                                    final Log log) {
        this.address = address;
        this.type = type;
        this.data = data;
        this.structureId = structureId;
        this.dataLength = dataLength;
        this.dataInsideSinglePage = dataInsideSinglePage;
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
        if (length >= 0) {
            return length;
        }

        assert log != null;
        length = log.loggableLength(address, end());
        return length;
    }

    @Override
    public long end() {
        if (end >= 0) {
            return end;
        }

        assert log != null;
        end = log.adjustedLoggableAddress(data.getDataAddress(), dataLength);
        return end;
    }

    @NotNull
    @Override
    public ByteIterableWithAddress getData() {
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
        return dataInsideSinglePage;
    }
}
