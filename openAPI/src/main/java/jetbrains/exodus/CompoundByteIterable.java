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

/**
 * A compound {@link ByteIterable} that can be composed of several sub-iterables.
 */
public class CompoundByteIterable extends ByteIterableBase {

    private final ByteIterable[] iterables;
    private final int count;

    public CompoundByteIterable(ByteIterable[] iterables) {
        this(iterables, iterables.length);
    }

    public CompoundByteIterable(ByteIterable[] iterables, int count) {
        if (count < 1) {
            throw new ExodusException("Failed to initialize CompoundByteIterable");
        }
        this.iterables = iterables;
        this.count = count;
    }

    @Override
    public int getLength() {
        int result = length;
        if (result == -1) {
            result = 0;
            for (int i = 0; i < count; ++i) {
                result += iterables[i].getLength();
            }
            length = result;
        }
        return result;
    }

    @Override
    protected ByteIterator getIterator() {
        return new CompoundByteIteratorBase(iterables[0].iterator()) {
            int off = 0;

            @Override
            public ByteIterator nextIterator() {
                off++;
                return off < count ? iterables[off].iterator() : null;
            }
        };
    }
}
