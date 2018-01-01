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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * {@code ContextualEnvironment} is always aware of transaction started in current thread. Its {@code openStore()}
 * methods return {@link ContextualStore} instances.
 *
 * @see Environment
 * @see ContextualStore
 */
public interface ContextualEnvironment extends Environment {

    /**
     * Opens existing or creates new {@linkplain Store store} with specified {@code name} and
     * {@linkplain StoreConfig config} inside transaction started in current thread. Internally, it calls
     * {@linkplain #getAndCheckCurrentTransaction()} to ensure that transaction started in current thread exists.
     * {@linkplain StoreConfig} provides meta-information used to create store. If it is known that the store
     * with specified name exists, then {@linkplain StoreConfig#USE_EXISTING} can be used.
     *
     * @param name   name of store
     * @param config {@linkplain StoreConfig} used to create store
     * @return {@linkplain ContextualStore} instance
     * @see ContextualStore
     * @see StoreConfig
     */
    @NotNull
    ContextualStore openStore(@NotNull String name, @NotNull StoreConfig config);

    /**
     * Opens existing or creates new {@linkplain Store store} with specified {@code name} and
     * {@linkplain StoreConfig config} inside transaction started in current thread. Internally, it calls
     * {@linkplain #getAndCheckCurrentTransaction()} to ensure that transaction started in current thread exists.
     * {@linkplain StoreConfig} provides meta-information used to create store. If it is known that the store
     * with specified name exists, then {@linkplain StoreConfig#USE_EXISTING} can be used.
     * <br><br>Pass {@code true} as {@code creationRequired} if creating new store is required or allowed. In that case,
     * the method will do the same as {@linkplain #openStore(String, StoreConfig)}. If you pass
     * {@code false} the method will return {@code null} for non-existing store.
     *
     * @param name             name of store
     * @param config           {@linkplain StoreConfig} used to create store
     * @param creationRequired pass {@code false} if you wish to get {@code null} for non-existing store
     *                         rather than create it.
     * @return {@linkplain ContextualStore} instance
     * @see ContextualStore
     * @see StoreConfig
     */
    @Nullable
    ContextualStore openStore(@NotNull String name, @NotNull StoreConfig config, final boolean creationRequired);

    @NotNull
    @Override
    ContextualStore openStore(@NotNull String name, @NotNull StoreConfig config, @NotNull Transaction transaction);

    @Nullable
    @Override
    ContextualStore openStore(@NotNull String name, @NotNull StoreConfig config, @NotNull Transaction transaction, boolean creationRequired);

    /**
     * @return {@linkplain Transaction transaction} instance or {@code null} if no transaction is started in current thread
     * @see Transaction
     */
    @Nullable
    Transaction getCurrentTransaction();

    /**
     * @return {@linkplain Transaction transaction} instance started in current thread
     * @throws jetbrains.exodus.ExodusException if there is no transaction started in current thread
     * @see Transaction
     */
    @NotNull
    Transaction getAndCheckCurrentTransaction();

    /**
     * This method is equivalent to {@linkplain Environment#getAllStoreNames(Transaction)}:
     * <pre>
     *     List<String> stores = getAllStoreNames(getAndCheckCurrentTransaction());
     * </pre>
     *
     * @return the list of names of all {@linkplain ContextualStore stores} created in the environment.
     * @see Environment#getAllStoreNames(Transaction)
     */
    @NotNull
    List<String> getAllStoreNames();
}
