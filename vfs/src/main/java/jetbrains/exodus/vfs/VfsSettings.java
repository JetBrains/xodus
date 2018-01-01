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
package jetbrains.exodus.vfs;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.Store;
import jetbrains.exodus.env.Transaction;
import jetbrains.exodus.env.TransactionalExecutable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * VFS persistent settings
 */
class VfsSettings {

    static final String NEXT_FREE_PATH_ID = "jetbrains.exodus.vfs.settings.nextFreePathId";

    @NotNull
    private final Environment env;
    @NotNull
    private final Store settingStore;

    VfsSettings(@NotNull final Environment env, @NotNull final Store settingStore) {
        this.env = env;
        this.settingStore = settingStore;
    }

    ByteIterable get(@Nullable final Transaction txn, @NotNull final String settingName) {
        final ArrayByteIterable key = StringBinding.stringToEntry(settingName);
        if (txn != null) {
            return settingStore.get(txn, key);
        }
        final ByteIterable[] result = new ByteIterable[1];
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                result[0] = settingStore.get(txn, key);
            }
        });
        return result[0];
    }

    void put(@Nullable final Transaction txn, @NotNull final String settingName, @NotNull final ByteIterable bi) {
        final ArrayByteIterable key = StringBinding.stringToEntry(settingName);
        if (txn != null) {
            settingStore.put(txn, key, bi);
        } else {
            env.executeInTransaction(new TransactionalExecutable() {
                @Override
                public void execute(@NotNull final Transaction txn) {
                    settingStore.put(txn, key, bi);
                }
            });
        }
    }
}
