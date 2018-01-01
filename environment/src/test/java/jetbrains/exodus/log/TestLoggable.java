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

import jetbrains.exodus.ByteIterable;
import org.jetbrains.annotations.NotNull;

/**
 * Loggable for writing
 */
final class TestLoggable implements Loggable {

    private final byte type;
    private final int structureId;
    @NotNull
    private final ByteIterable data;

    TestLoggable(final byte type, @NotNull final ByteIterable data, final int structureId) {
        this.type = type;
        this.data = data;
        this.structureId = structureId;
    }

    @Override
    public long getAddress() {
        throw new UnsupportedOperationException("TestLoggable has no address until it is written to log");
    }

    @Override
    public byte getType() {
        return type;
    }

    @Override
    public int length() {
        final int dataLength = getDataLength();
        return dataLength + 1 + CompressedUnsignedLongByteIterable.getCompressedSize(structureId) + CompressedUnsignedLongByteIterable.getCompressedSize(dataLength);
    }

    @NotNull
    @Override
    public ByteIterable getData() {
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
}
