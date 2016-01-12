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
package jetbrains.exodus.log;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.ExodusException;
import org.jetbrains.annotations.NotNull;

abstract class CompoundByteIteratorBase implements ByteIterator {

    @NotNull
    private ByteIterator current;
    private boolean hasNext;
    private boolean hasNextValid;

    protected CompoundByteIteratorBase(@NotNull ByteIterator current) {
        this.current = current;
    }

    protected CompoundByteIteratorBase() {
        this(ByteIterable.EMPTY_ITERATOR);
    }

    @Override
    public boolean hasNext() {
        if (!hasNextValid) {
            hasNext = hasNextImpl();
            hasNextValid = true;
        }
        return hasNext;
    }

    @Override
    public long skip(final long length) {
        long skipped = current.skip(length);
        while (true) {
            hasNextValid = false;
            if (skipped >= length || !hasNext()) {
                break;
            }
            skipped += current.skip(length - skipped);
        }
        return skipped;
    }

    @Override
    public byte next() {
        if (!hasNext()) {
            onFail("CompoundByteIterator: no more bytes available");
        }
        final byte result = current.next();
        hasNextValid = false;
        return result;
    }

    private boolean hasNextImpl() {
        while (!current.hasNext()) {
            final ByteIterator nextIterator = nextIterator();
            if (nextIterator == null) {
                return false;
            }
            current = nextIterator;
        }
        return true;
    }

    @NotNull
    protected ByteIterator getCurrent() {
        return current;
    }

    /**
     * @return null to finish.
     */
    protected abstract ByteIterator nextIterator();

    protected void onFail(@NotNull final String message) throws ExodusException {
        throw new ExodusException(message);
    }
}
