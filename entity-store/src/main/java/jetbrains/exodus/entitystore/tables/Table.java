/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
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
package jetbrains.exodus.entitystore.tables;

import jetbrains.exodus.entitystore.PersistentEntityStoreImpl;
import org.jetbrains.annotations.NonNls;
import org.jetbrains.annotations.NotNull;

public abstract class Table {
    @NonNls
    public static final String ALL_IDX = "#all_idx";

    public static void checkStatus(final boolean success, @NotNull final String message) {
        if (!success) {
            PersistentEntityStoreImpl.loggerWarn(message + ", operation unsuccessful", new Throwable());
        }
    }

    public abstract boolean canBeCached();
}
