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
package jetbrains.exodus.env;

import jetbrains.exodus.ByteIterable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface ContextualStore extends Store {

    @Nullable
    ByteIterable get(@NotNull final ByteIterable key);

    boolean exists(@NotNull final ByteIterable key, @NotNull final ByteIterable data);

    boolean put(@NotNull final ByteIterable key, @NotNull final ByteIterable value);

    void putRight(@NotNull final ByteIterable key, @NotNull final ByteIterable value);

    boolean add(@NotNull final ByteIterable key, @NotNull final ByteIterable value);

    boolean delete(@NotNull final ByteIterable key);

    long count();

    Cursor openCursor();

}
