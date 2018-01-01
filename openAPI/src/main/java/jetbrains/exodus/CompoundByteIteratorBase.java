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
package jetbrains.exodus;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Base iterator for {@link CompoundByteIterable}.
 */
public abstract class CompoundByteIteratorBase extends ByteIterator {

    @Nullable
    private ByteIterator current;

    protected CompoundByteIteratorBase(@NotNull ByteIterator current) {
        this.current = current;
    }

    protected CompoundByteIteratorBase() {
        this(ByteIterable.EMPTY_ITERATOR);
    }

    @Override
    public boolean hasNext() {
        if (current == null) {
            return false;
        }
        if (current.hasNext()) {
            return true;
        }
        current = nextIterator();
        return hasNext();
    }

    @Override
    public long skip(final long length) {
        long skipped = 0;
        while (current != null) {
            skipped += current.skip(length - skipped);
            if (skipped >= length || !hasNext()) {
                break;
            }
        }
        return skipped;
    }

    @Override
    public byte next() {
        hasNext();
        //noinspection ConstantConditions
        return current.next();
    }

    @NotNull
    protected ByteIterator getCurrent() {
        if (current == null) {
            throw new ExodusException("Can't get current ByteIterator, hasNext() == false");
        }
        return current;
    }

    /**
     * @return null to finish.
     */
    protected abstract ByteIterator nextIterator();
}
