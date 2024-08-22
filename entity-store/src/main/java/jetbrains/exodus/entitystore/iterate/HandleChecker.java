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
package jetbrains.exodus.entitystore.iterate;

import jetbrains.exodus.entitystore.EntityIterableHandle;
import jetbrains.exodus.entitystore.PersistentStoreTransaction;
import jetbrains.exodus.entitystore.Updatable;
import org.jetbrains.annotations.NotNull;

public interface HandleChecker {
    int getLinkId();

    int getPropertyId();

    int getTypeId();

    int getTypeIdAffectingCreation();

    @NotNull
    PersistentStoreTransaction getTxn();

    void beginUpdate(@NotNull Updatable instance);

    @Deprecated
    Updatable getUpdatableIterable(@NotNull final EntityIterableHandle handle);
}
