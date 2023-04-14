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

import jetbrains.exodus.bindings.BindingUtils;
import org.jetbrains.annotations.NotNull;

public class HashCodeLoggable implements RandomAccessLoggable {
    public static final byte TYPE = 0x7F;

    private final long address;

    private final long hashCode;

    HashCodeLoggable(long address, int pageOffset, byte[] page) {
        this.address = address;
        this.hashCode = BindingUtils.readLong(page, pageOffset);
    }


    @Override
    public long getAddress() {
        return address;
    }

    @Override
    public byte getType() {
        return TYPE;
    }

    @Override
    public int length() {
        return Long.BYTES + 1;
    }

    @Override
    public long end() {
        return address + length();
    }

    @Override
    public @NotNull ByteIterableWithAddress getData() {
        final byte[] bytes = new byte[Long.BYTES];
        BindingUtils.writeLong(hashCode, bytes, 0);

        return new ArrayByteIterableWithAddress(address + Long.BYTES, bytes, 0, bytes.length);
    }

    @Override
    public int getDataLength() {
        return Long.BYTES;
    }

    public long getHashCode() {
        return hashCode;
    }

    @Override
    public int getStructureId() {
        return NO_STRUCTURE_ID;
    }

    @Override
    public boolean isDataInsideSinglePage() {
        return true;
    }

    public static boolean isHashCodeLoggable(final byte type) {
        return type == TYPE;
    }

    public static boolean isHashCodeLoggable(final Loggable loggable) {
        return loggable.getType() == TYPE;
    }
}
