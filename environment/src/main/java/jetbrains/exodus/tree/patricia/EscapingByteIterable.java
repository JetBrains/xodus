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
package jetbrains.exodus.tree.patricia;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterableBase;
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.ExodusException;
import org.jetbrains.annotations.NotNull;

/**
 * Escapes zeros in origin byte iterable.
 */
final class EscapingByteIterable extends ByteIterableBase {

    static final byte ESCAPING_BYTE = 1;

    @NotNull
    private final ByteIterable origin;

    EscapingByteIterable(@NotNull final ByteIterable origin) {
        this.origin = origin;
    }

    @Override
    protected ByteIterator getIterator() {
        return new ByteIterator() {

            private final ByteIterator originIt = origin.iterator();
            private boolean hasEscaped = false;
            private byte escaped = 0;

            @Override
            public boolean hasNext() {
                return hasEscaped || originIt.hasNext();
            }

            @Override
            public byte next() {
                if (hasEscaped) {
                    hasEscaped = false;
                    return escaped;
                }
                final byte nextByte = originIt.next();
                if (nextByte == 0 || nextByte == 1) {
                    hasEscaped = true;
                    escaped = (byte) (nextByte + 1);
                    return ESCAPING_BYTE;
                }
                return nextByte;
            }

            @Override
            public long skip(long bytes) {
                throw new UnsupportedOperationException();
            }
        };
    }

}

final class UnEscapingByteIterable extends ByteIterableBase {

    @NotNull
    private final ByteIterable origin;

    UnEscapingByteIterable(@NotNull final ByteIterable origin) {
        this.origin = origin;
    }

    @Override
    protected ByteIterator getIterator() {
        return new ByteIterator() {

            private final ByteIterator originIt = origin.iterator();

            @Override
            public boolean hasNext() {
                return originIt.hasNext();
            }

            @Override
            public byte next() {
                byte nextByte = originIt.next();
                if (nextByte == EscapingByteIterable.ESCAPING_BYTE) {
                    if (!originIt.hasNext()) {
                        throw new ExodusException("No byte follows escaping byte");
                    }
                    nextByte = (byte) (originIt.next() - 1);
                }
                return nextByte;
            }

            @Override
            public long skip(long bytes) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
