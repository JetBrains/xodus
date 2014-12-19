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

import jetbrains.exodus.ByteIterable;
import org.jetbrains.annotations.NotNull;

public abstract class ByteIterableWithAddress implements ByteIterable {

    public static final ByteIterableWithAddress EMPTY = new ArrayByteIterableWithAddress(0, ByteIterable.EMPTY_BYTES, 0, 0);

    private final long address;

    protected ByteIterableWithAddress(final long address) {
        this.address = address;
    }

    public final long getAddress() {
        return address;
    }

    @Override
    public abstract ByteIteratorWithAddress iterator();

    public abstract ByteIteratorWithAddress iterator(final int offset);

    public abstract int compareTo(final int offset, final int len, @NotNull final ByteIterable right);

    public abstract ByteIterableWithAddress clone(final int offset);

    @Override
    public final byte[] getBytesUnsafe() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final int getLength() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final int compareTo(@NotNull final ByteIterable right) {
        // can't compare
        throw new UnsupportedOperationException();
    }
}
