/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.log.iterate.CompoundByteIteratorBase;
import org.jetbrains.annotations.NotNull;

final class DataIterator extends CompoundByteIteratorBase {

    @NotNull
    private final Log log;
    private long currentAddress;

    DataIterator(@NotNull final Log log, final long startAddress) {
        super(ArrayByteIterable.getEmptyIterator());
        this.log = log;
        currentAddress = startAddress;
    }

    @Override
    protected ByteIterator nextIterator() {
        final long prevAddress = getHighAddress();
        final int alignment = ((int) prevAddress) & (log.getCachePageSize() - 1);
        final ArrayByteIterable page;
        final long newAddress = prevAddress - alignment;
        try {
            page = log.cache.getPage(log, newAddress);
        } catch (BlockNotFoundException e) {
            return null;
        }
        final int readBytes = page.getLength();
        if (readBytes <= alignment) { // alignment is >= 0 for sure
            return null;
        }
        currentAddress = newAddress;
        return page.iterator(alignment);
    }

    public long getHighAddress() {
        return currentAddress + getCurrent().getOffset();
    }

    @NotNull
    @Override
    public ArrayByteIterable.Iterator getCurrent() {
        return (ArrayByteIterable.Iterator) super.getCurrent();
    }

    @Override
    protected void onFail(@NotNull String message) throws ExodusException {
        super.onFail(message + ", address = " + getHighAddress());
    }
}
