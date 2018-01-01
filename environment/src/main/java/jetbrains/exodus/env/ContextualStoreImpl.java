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
package jetbrains.exodus.env;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.tree.TreeMetaInfo;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ContextualStoreImpl extends StoreImpl implements ContextualStore {

    @NotNull
    private final ContextualEnvironmentImpl environment;

    public ContextualStoreImpl(@NotNull ContextualEnvironmentImpl env, @NotNull String name, @NotNull TreeMetaInfo metaInfo) {
        super(env, name, metaInfo);
        this.environment = env;
    }

    @NotNull
    @Override
    public ContextualEnvironmentImpl getEnvironment() {
        return environment;
    }

    @Nullable
    public ByteIterable get(@NotNull final ByteIterable key) {
        return get(environment.getAndCheckCurrentTransaction(), key);
    }

    public boolean exists(@NotNull final ByteIterable key, @NotNull final ByteIterable value) {
        return exists(environment.getAndCheckCurrentTransaction(), key, value);
    }

    public boolean put(@NotNull final ByteIterable key, @NotNull final ByteIterable value) {
        return put(environment.getAndCheckCurrentTransaction(), key, value);
    }

    public void putRight(@NotNull final ByteIterable key, @NotNull final ByteIterable value) {
        putRight(environment.getAndCheckCurrentTransaction(), key, value);
    }

    public boolean add(@NotNull final ByteIterable key, @NotNull final ByteIterable value) {
        return add(environment.getAndCheckCurrentTransaction(), key, value);
    }

    public boolean delete(@NotNull final ByteIterable key) {
        return delete(environment.getAndCheckCurrentTransaction(), key);
    }

    public long count() {
        return count(environment.getAndCheckCurrentTransaction());
    }

    public Cursor openCursor() {
        return openCursor(environment.getAndCheckCurrentTransaction());
    }
}
