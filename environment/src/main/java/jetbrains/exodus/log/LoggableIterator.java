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

import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

/**
 * Stops at the end of log or on the file hole
 */
final class LoggableIterator implements Iterator<Loggable> {

    @NotNull
    private final Log log;
    @NotNull
    private final DataIterator it;
    private int skip;

    LoggableIterator(@NotNull final Log log, final long startAddress) {
        this.log = log;
        it = log.readIteratorFrom(startAddress);
        skip = 0;
    }

    @Override
    public RandomAccessLoggable next() {
        if (!hasNext()) {
            return null;
        }
        final RandomAccessLoggable result = log.read(it);
        skip = (NullLoggable.isNullLoggable(result)) ? 0 : result.getDataLength();
        return result;
    }

    @Override
    public boolean hasNext() {
        skip();
        return it.hasNext();
    }

    @Override
    public void remove() {
    }

    private void skip() {
        if (skip > 0) {
            it.skip(skip);
            skip = 0;
        }
    }
}
