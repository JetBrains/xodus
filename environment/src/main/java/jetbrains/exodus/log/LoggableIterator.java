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

import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

/**
 * Stops at the end of log or on the file hole
 */
public final class LoggableIterator implements Iterator<RandomAccessLoggable> {

    @NotNull
    private final Log log;
    @NotNull
    private final ByteIteratorWithAddress it;

    public LoggableIterator(@NotNull final Log log, final long startAddress) {
        this(log, log.readIteratorFrom(startAddress));
    }

    public LoggableIterator(@NotNull final Log log, @NotNull final ByteIteratorWithAddress it) {
        this.log = log;
        this.it = it;
    }

    public long getHighAddress() {
        return it.getAddress();
    }

    @Override
    public RandomAccessLoggable next() {
        if (!hasNext()) {
            return null;
        }
        var address = it.getAddress();
        final RandomAccessLoggable result = log.read(it);
        it.skip(result.length() - (it.getAddress() - address));

        return result;
    }

    @Override
    public boolean hasNext() {
        return it.hasNext();
    }

    @Override
    public void remove() {
    }
}
