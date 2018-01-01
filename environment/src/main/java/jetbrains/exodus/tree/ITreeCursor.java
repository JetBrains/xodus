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
import jetbrains.exodus.env.Cursor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface ITreeCursor extends Cursor {

    boolean isMutable();

    ITree getTree();

    ITreeCursor EMPTY_CURSOR = new ITreeCursor() {

        @Override
        public boolean isMutable() {
            return false;
        }

        @Override
        public boolean getNext() {
            return false;
        }

        @Override
        public boolean getNextDup() {
            return false;
        }

        @Override
        public boolean getNextNoDup() {
            return false;
        }

        @Override
        public boolean getLast() {
            return false;
        }

        @Override
        public boolean getPrev() {
            return false;
        }

        @Override
        public boolean getPrevDup() {
            return false;
        }

        @Override
        public boolean getPrevNoDup() {
            return false;
        }

        @Override
        @NotNull
        public ByteIterable getKey() {
            throw new UnsupportedOperationException("No key found");
        }

        @Override
        @NotNull
        public ByteIterable getValue() {
            throw new UnsupportedOperationException("No value found");
        }

        @Override
        @Nullable
        public ByteIterable getSearchKey(@NotNull ByteIterable key) {
            return null;
        }

        @Override
        @Nullable
        public ByteIterable getSearchKeyRange(@NotNull ByteIterable key) {
            return null;
        }

        @Override
        public boolean getSearchBoth(@NotNull ByteIterable key, @NotNull ByteIterable value) {
            return false;
        }

        @Override
        @Nullable
        public ByteIterable getSearchBothRange(@NotNull ByteIterable key, @NotNull ByteIterable value) {
            return null;
        }

        @Override
        public int count() {
            return 0;
        }

        @Override
        public void close() {
        }

        @Override
        public ITree getTree() {
            return null;
        }

        @Override
        public boolean deleteCurrent() {
            return false;
        }
    };
}
