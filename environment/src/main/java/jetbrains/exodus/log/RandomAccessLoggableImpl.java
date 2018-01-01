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

public class RandomAccessLoggableImpl implements RandomAccessLoggable {

    private final byte type;
    private final byte headerLength;
    private final int length;
    @NotNull
    private final ByteIterableWithAddress data;
    private final int structureId;

    @SuppressWarnings({"ConstructorWithTooManyParameters"})
    public RandomAccessLoggableImpl(final long address,
                                    final byte type,
                                    @NotNull final ByteIterableWithAddress data,
                                    final int dataLength,
                                    final int structureId) {
        this.type = type;
        this.headerLength = (byte) (data.getDataAddress() - address);
        this.length = dataLength + headerLength;
        this.data = data;
        this.structureId = structureId;
    }

    @Override
    public long getAddress() {
        return data.getDataAddress() - headerLength;
    }

    @Override
    public byte getType() {
        return type;
    }

    @Override
    public int length() {
        return length;
    }

    @NotNull
    @Override
    public ByteIterableWithAddress getData() {
        return data;
    }

    public int getDataLength() {
        return length - headerLength;
    }

    @Override
    public int getStructureId() {
        return structureId;
    }
}
