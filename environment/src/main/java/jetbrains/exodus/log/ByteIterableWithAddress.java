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

import jetbrains.exodus.ByteIterable;
import org.jetbrains.annotations.NotNull;

public abstract class ByteIterableWithAddress implements ByteIterable {

    public static final ByteIterableWithAddress EMPTY = getEmpty(Loggable.NULL_ADDRESS);

    private final long address;

    protected ByteIterableWithAddress(final long address) {
        this.address = address;
    }

    public final long getDataAddress() {
        return address;
    }

    protected byte byteAt(final int offset) {
        return iterator(offset).next();
    }

    public final byte byteAtAddress(final long address) {
        final int offset = (int) (address - this.address);
        return byteAt(offset);
    }

    protected long nextLong(final int offset, final int length) {
        return iterator(offset).nextLong(length);
    }

    public final long nextLongByAddress(final long address, final int length) {
        final int offset = (int) (address - this.address);
        return nextLong(offset, length);
    }

    public int getCompressedUnsignedInt() {
        return CompressedUnsignedLongByteIterable.getInt(this);
    }

    @Override
    public abstract ByteIteratorWithAddress iterator();

    public abstract ByteIteratorWithAddress iterator(final int offset);

    public abstract int compareTo(final int offset, final int len, @NotNull final ByteIterable right);

    public abstract ByteIterableWithAddress clone(final int offset);

    @Override
    public byte[] getBytesUnsafe() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getLength() {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public ByteIterable subIterable(final int offset, final int length) {
        return new LogAwareFixedLengthByteIterable(this, offset, length);
    }

    @Override
    public int compareTo(@NotNull final ByteIterable right) {
        // can't compare
        throw new UnsupportedOperationException();
    }

    static ByteIterableWithAddress getEmpty(final long address) {
        return new ArrayByteIterableWithAddress(address, ByteIterable.EMPTY_BYTES, 0, 0);
    }
}
