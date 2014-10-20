/**
 * Copyright 2010 - 2014 JetBrains s.r.o.
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

import jetbrains.exodus.Backupable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

public interface Environment extends Backupable {

    long getCreated();

    @NotNull
    String getLocation();

    @NotNull
    Store openStore(@NotNull String name, @NotNull StoreConfig config, @NotNull Transaction transaction);

    @Nullable
    Store openStore(@NotNull String name, @NotNull StoreConfig config, @NotNull Transaction transaction, boolean creationRequired);

    /**
     * Executes a task after all currently started transactions are finished.
     *
     * @param task task to execute
     */
    void executeTransactionSafeTask(@NotNull Runnable task);

    void clear();

    void close();

    boolean isOpen();

    @NotNull
    List<String> getAllStoreNames(@NotNull Transaction transaction);

    boolean storeExists(@NotNull String storeName, @NotNull Transaction transaction);

    void truncateStore(@NotNull String storeName, @NotNull Transaction transaction);

    void removeStore(@NotNull String storeName, @NotNull Transaction transaction);

    long getDiskUsage();

    void gc();

    void suspendGC();

    void resumeGC();

    @NotNull
    Transaction beginTransaction();

    @NotNull
    Transaction beginTransaction(Runnable beginHook);

    /**
     * Starts a read-only transaction in which any writing attempt fails.
     *
     * @return read-only transaction object.
     */
    @NotNull
    Transaction beginReadonlyTransaction();

    @NotNull
    Transaction beginReadonlyTransaction(Runnable beginHook);

    void executeInTransaction(@NotNull TransactionalExecutable executable);

    void executeInReadonlyTransaction(@NotNull TransactionalExecutable executable);

    <T> T computeInTransaction(@NotNull TransactionalComputable<T> computable);

    <T> T computeInReadonlyTransaction(@NotNull TransactionalComputable<T> computable);

    EnvironmentConfig getEnvironmentConfig();
}
