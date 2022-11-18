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

public interface ByteIterableWithAddress extends ByteIterable {

    ByteIterableWithAddress EMPTY = getEmpty(Loggable.NULL_ADDRESS);

    long getDataAddress();

    byte byteAt(final int offset);

    long nextLong(final int offset, final int length);

    int getCompressedUnsignedInt();

    @Override
    ByteIteratorWithAddress iterator();

    ByteIteratorWithAddress iterator(final int offset);

    ByteIterableWithAddress cloneWithOffset(final int offset);

    ByteIterableWithAddress cloneWithAddressAndLength(final long address, final int length);

    static ByteIterableWithAddress getEmpty(final long address) {
        return new ArrayByteIterableWithAddress(address, ByteIterable.EMPTY_BYTES, 0, 0);
    }
}
