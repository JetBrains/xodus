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
package jetbrains.exodus.entitystore;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.env.Store;
import jetbrains.exodus.env.Transaction;
import jetbrains.exodus.env.TransactionalExecutable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class Settings {

    private Settings() {
    }

    @Nullable
    public static String get(@NotNull final Store settingsStore, @NotNull final String name) {
        final ByteIterable[] result = new ByteIterable[1];
        try {
            settingsStore.getEnvironment().executeInTransaction(new TransactionalExecutable() {
                @Override
                public void execute(@NotNull final Transaction txn) {
                    result[0] = settingsStore.get(txn, StringBinding.stringToEntry(name));
                }
            });
        } catch (ExodusException e) {
            // ignore
        }
        if (result[0] == null) {
            return null;
        }
        return StringBinding.entryToString(result[0]);
    }

    public static void set(@NotNull final Store settingsStore, @NotNull final String name, @NotNull final String setting) {
        settingsStore.getEnvironment().executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                settingsStore.put(txn, StringBinding.stringToEntry(name), StringBinding.stringToEntry(setting));
            }
        });
    }

    public static void delete(@NotNull final Store settingsStore, @NotNull final String name) {
        settingsStore.getEnvironment().executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                settingsStore.delete(txn, StringBinding.stringToEntry(name));
            }
        });
    }
}
