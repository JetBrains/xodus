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
package jetbrains.exodus.tree;

import jetbrains.exodus.ByteIterable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.PrintStream;

public interface INode extends Dumpable {
    boolean hasValue();

    @NotNull
    ByteIterable getKey();

    @Nullable
    ByteIterable getValue();

    INode EMPTY = new INode() {
        @Override
        public boolean hasValue() {
            return false;
        }

        @NotNull
        @Override
        public ByteIterable getKey() {
            return ByteIterable.EMPTY;
        }

        @Override
        public ByteIterable getValue() {
            return ByteIterable.EMPTY;
        }

        @Override
        public void dump(PrintStream out, int level, ToString renderer) {
            out.println("Empty node");
        }
    };

}
