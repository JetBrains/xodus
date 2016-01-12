/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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
package jetbrains.exodus.env;

import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.log.*;
import jetbrains.exodus.util.LightOutputStream;
import org.jetbrains.annotations.NotNull;

final class DatabaseRoot extends RandomAccessLoggableImpl {

    public static final byte DATABASE_ROOT_TYPE = 1;

    private static final long MAGIC_DIFF = 199L;
    private static final LoggableFactory ROOT_FACTORY = new LoggableFactory() {
        @Override
        protected RandomAccessLoggable create(final long address,
                                              @NotNull final ByteIterableWithAddress data,
                                              final int dataLength,
                                              final int structureId) {
            final ByteIterator it = data.iterator();
            final long rootAddress = CompressedUnsignedLongByteIterable.getLong(it);
            final int lastStructureId = CompressedUnsignedLongByteIterable.getInt(it);
            final boolean isValid = rootAddress ==
                    CompressedUnsignedLongByteIterable.getLong(it) - lastStructureId - MAGIC_DIFF;
            return new DatabaseRoot(rootAddress, lastStructureId, isValid, address, data, dataLength);
        }
    };

    private final long rootAddress;
    private final int lastStructureId;
    private final boolean isValid;

    @SuppressWarnings({"ConstructorWithTooManyParameters"})
    private DatabaseRoot(final long rootAddress,
                         final int lastStructureId,
                         final boolean isValid,
                         final long address,
                         @NotNull final ByteIterableWithAddress data,
                         final int dataLength) {
        super(address, DATABASE_ROOT_TYPE, data, dataLength, NO_STRUCTURE_ID);
        this.rootAddress = rootAddress;
        this.lastStructureId = lastStructureId;
        this.isValid = isValid;
    }

    long getRootAddress() {
        return rootAddress;
    }

    public int getLastStructureId() {
        return lastStructureId;
    }

    public boolean isValid() {
        return isValid;
    }

    static void register() {
        LoggableFactory.registerLoggable(DATABASE_ROOT_TYPE, ROOT_FACTORY);
    }

    static Loggable toLoggable(final long rootAddress, final int lastStructureId) {
        final LightOutputStream output = new LightOutputStream(20);
        CompressedUnsignedLongByteIterable.fillBytes(rootAddress, output);
        CompressedUnsignedLongByteIterable.fillBytes(lastStructureId, output);
        CompressedUnsignedLongByteIterable.fillBytes(rootAddress + lastStructureId + MAGIC_DIFF, output);
        return new LoggableToWrite(DATABASE_ROOT_TYPE, output.asArrayByteIterable());
    }

}
