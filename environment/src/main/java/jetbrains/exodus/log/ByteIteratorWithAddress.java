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

import jetbrains.exodus.ByteIterator;

public interface ByteIteratorWithAddress extends ByteIterator {

    ByteIteratorWithAddress EMPTY = new ByteIteratorWithAddress() {

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public byte next() {
            return (byte) 0;
        }

        @Override
        public long getAddress() {
            return Loggable.NULL_ADDRESS;
        }

        @Override
        public long skip(long length) {
            return 0;
        }
    };

    long getAddress();
}
