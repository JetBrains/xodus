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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

public interface ContextualEnvironment extends Environment {

    @NotNull
    ContextualStore openStore(@NotNull String name, @NotNull StoreConfig config);

    @Nullable
    ContextualStore openStore(@NotNull String name, @NotNull StoreConfig config, final boolean creationRequired);

    @NotNull
    @Override
    ContextualStore openStore(@NotNull String name, @NotNull StoreConfig config, @NotNull Transaction transaction);

    @Nullable
    @Override
    ContextualStore openStore(@NotNull String name, @NotNull StoreConfig config, @NotNull Transaction transaction, boolean creationRequired);

    /**
     * Returns transaction started in current thread if any.
     *
     * @return TransactionDescriptor object.
     */
    @Nullable
    Transaction getCurrentTransaction();

    /**
     * Returns transaction started in current thread or throws an ExodusException if no transaction was started.
     *
     * @return TransactionDescriptor object.
     */
    @NotNull
    Transaction getAndCheckCurrentTransaction();

    @NotNull
    List<String> getAllStoreNames();

}
