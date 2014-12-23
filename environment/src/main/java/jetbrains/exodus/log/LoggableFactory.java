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

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.log.iterate.CompressedUnsignedLongByteIterable;
import org.jetbrains.annotations.NotNull;

public abstract class LoggableFactory {

    private static final int MAXIMUM_LOGGABLE_TYPE = 128;
    private static final LoggableFactory[] FACTORY = new LoggableFactory[MAXIMUM_LOGGABLE_TYPE];

    protected LoggableFactory() {
    }

    public static void registerLoggable(final int type, final LoggableFactory loggableFactory) {
        final LoggableFactory prevLoggableFactory = FACTORY[type];
        if (prevLoggableFactory != null && prevLoggableFactory != loggableFactory) {
            throw new ExodusException("Loggable type is already registered " + type);
        }
        FACTORY[type] = loggableFactory;
    }

    public static void clear() {
        for (int i = 0; i < FACTORY.length; i++) {
            FACTORY[i] = null;
        }
    }

    static RandomAccessLoggable create(@NotNull final Log log, @NotNull final DataIterator it, final long address) {
        final byte type = (byte) (it.next() ^ 0x80);
        if (NullLoggable.isNullLoggable(type)) {
            return new NullLoggable(address);
        }
        if (type < 0 || type >= FACTORY.length) {
            throw new ExodusException("Invalid loggable type");
        }
        final LoggableFactory prototype = FACTORY[type];
        final long structureId = CompressedUnsignedLongByteIterable.getLong(it);
        final int dataLength = CompressedUnsignedLongByteIterable.getInt(it);
        final long dataAddress = it.getHighAddress();
        final int headerLength = (int) (dataAddress - address);
        final int length = dataLength + headerLength;
        final ArrayByteIterable.Iterator currentPageIt = it.getCurrent();
        final byte[] currentPage = currentPageIt.getBytesUnsafe();
        final int currentOffset = currentPageIt.getOffset();
        final boolean fitsInSinglePage = currentPageIt.getLength() - currentOffset >= dataLength;
        if (prototype != null) {
            final ByteIterableWithAddress data;
            if (dataLength == 0) {
                data = ByteIterableWithAddress.EMPTY;
            } else if (fitsInSinglePage) {
                data = new ArrayByteIterableWithAddress(dataAddress, currentPage, currentOffset, dataLength);
            } else {
                data = new RandomAccessByteIterable(dataAddress, log);
            }
            return prototype.create(address, length, data, dataLength, structureId);
        }
        if (fitsInSinglePage) {
            return new RandomAccessLoggableAndArrayByteIterable(
                    address, type, length, structureId, dataAddress, currentPage, currentOffset, dataLength);
        }
        return new RandomAccessLoggableImpl(
                address, type, length, new RandomAccessByteIterable(dataAddress, log), dataLength, structureId);
    }

    /**
     * Creates new instance of loggable.
     *
     * @param address     address of result in the log.
     * @param length      length of the loggable.
     * @param data        loggable data.
     * @param dataLength  length of the loggable data.
     * @param structureId structure id of loggable.
     * @return new loggable instance.
     */
    protected abstract RandomAccessLoggable create(final long address,
                                                   final int length,
                                                   @NotNull final ByteIterableWithAddress data,
                                                   final int dataLength,
                                                   final long structureId);
}
