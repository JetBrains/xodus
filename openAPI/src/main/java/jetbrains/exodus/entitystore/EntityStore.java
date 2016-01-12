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
package jetbrains.exodus.entitystore;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface EntityStore {

    /**
     * User-friendly name of the EntityStore.
     *
     * @return name of the EntityStore.
     */
    @NotNull
    String getName();

    /**
     * Absolute location (path) of the EntityStore.
     *
     * @return where the EntityStore's files exist.
     */
    @NotNull
    String getLocation();

    /**
     * Starts a new transaction on the store.
     *
     * @return new store transaction object.
     */
    @NotNull
    StoreTransaction beginTransaction();

    /**
     * Starts a new readonly transaction on the store.
     *
     * @return new store transaction object.
     */
    @NotNull
    StoreTransaction beginReadonlyTransaction();

    /**
     * Gets current started transaction object.
     *
     * @return last started transaction or null if no transaction was started.
     */
    @Nullable
    StoreTransaction getCurrentTransaction();

    /**
     * Closes the EntityStore.
     */
    void close();
}
