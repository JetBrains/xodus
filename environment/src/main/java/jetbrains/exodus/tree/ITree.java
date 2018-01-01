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
import jetbrains.exodus.log.DataIterator;
import jetbrains.exodus.log.Log;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.PrintStream;

/**
 * Interface for immutable tree implementations
 */
public interface ITree {

    @NotNull
    Log getLog();

    @NotNull
    DataIterator getDataIterator(long address);

    long getRootAddress();

    int getStructureId();

    @Nullable
    ByteIterable get(@NotNull final ByteIterable key);

    boolean hasPair(@NotNull final ByteIterable key, @NotNull final ByteIterable value);

    boolean hasKey(@NotNull final ByteIterable key);

    @NotNull
    ITreeMutable getMutableCopy();

    boolean isEmpty();

    long getSize();

    ITreeCursor openCursor();

    LongIterator addressIterator();

    void dump(PrintStream out);

    void dump(PrintStream out, INode.ToString renderer);
}
