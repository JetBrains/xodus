/*
 * Copyright 2010 - 2024 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class Settings {

    private Settings() {
    }

    @Nullable
    public static String get(@NotNull final Store settingsStore, @NotNull final String name) {
        return settingsStore.getEnvironment().computeInTransaction(txn -> get(txn, settingsStore, name));
    }

    @Nullable
    public static String get(@NotNull final Transaction txn, @NotNull final Store settingsStore, @NotNull final String name) {
        try {
            final ByteIterable entry = settingsStore.get(txn, StringBinding.stringToEntry(name));
            if (entry != null) {
                return StringBinding.entryToString(entry);
            }
        } catch (ExodusException ignore) {
        }
        return null;
    }

    public static void set(@NotNull final Store settingsStore, @NotNull final String name, @NotNull final String setting) {
        settingsStore.getEnvironment().executeInTransaction(txn -> set(txn, settingsStore, name, setting));
    }

    public static void set(@NotNull final Transaction txn, @NotNull final Store settingsStore, @NotNull final String name, @NotNull final String setting) {
        settingsStore.put(txn, StringBinding.stringToEntry(name), StringBinding.stringToEntry(setting));
    }

    public static void delete(@NotNull final Store settingsStore, @NotNull final String name) {
        settingsStore.getEnvironment().executeInTransaction(txn -> settingsStore.delete(txn, StringBinding.stringToEntry(name)));
    }
}
