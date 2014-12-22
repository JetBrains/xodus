/**
 * Copyright 2010 - 2014 JetBrains s.r.o.
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

public class RandomAccessLoggable implements Loggable {

    private final long address;
    private final byte type;
    private final byte headerLength;
    private final int length;
    @NotNull
    private final ByteIterableWithAddress data;
    private final int structureId;

    @SuppressWarnings({"ConstructorWithTooManyParameters"})
    public RandomAccessLoggable(final long address,
                                final int type,
                                final int length,
                                @NotNull final ByteIterableWithAddress data,
                                final int dataLength,
                                final long structureId) {
        this.address = address;
        this.type = (byte) type;
        headerLength = (byte) (length - dataLength);
        this.length = length;
        this.data = data;
        this.structureId = (int) structureId;
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

    @NotNull
    @Override
    public ByteIterableWithAddress getData() {
        return data;
    }

    public byte getHeaderLength() {
        return headerLength;
    }

    @Override
    public int getDataLength() {
        return length - headerLength;
    }

    @Override
    public long getStructureId() {
        return structureId;
    }
}
