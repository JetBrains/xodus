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

import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.EnvironmentConfig;
import jetbrains.exodus.env.Environments;
import org.jetbrains.annotations.NonNls;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;

public final class PersistentEntityStores {

    @NonNls
    private static final String DEFAULT_NAME = "persistentEntityStore";
    private static final int STORE_GET_CACHE_SIZE = 65536;
    private static final int TREE_NODES_CACHE_SIZE = 0;

    private PersistentEntityStores() {
    }

    public static PersistentEntityStoreImpl newInstance(@NotNull final PersistentEntityStoreConfig config,
                                                        @NotNull final Environment environment,
                                                        @Nullable final BlobVault blobVault,
                                                        @NotNull final String name) {
        return new PersistentEntityStoreImpl(config, environment, blobVault, name);
    }

    public static PersistentEntityStoreImpl newInstance(@NotNull final Environment environment,
                                                        @Nullable final BlobVault blobVault,
                                                        @NotNull final String name) {
        return newInstance(new PersistentEntityStoreConfig(), environment, blobVault, name);
    }

    public static PersistentEntityStoreImpl newInstance(@NotNull final PersistentEntityStoreConfig config,
                                                        @NotNull final Environment environment,
                                                        @NotNull final String name) {
        return new PersistentEntityStoreImpl(config, environment, null, name);
    }

    public static PersistentEntityStoreImpl newInstance(@NotNull final Environment environment, @NotNull final String name) {
        return newInstance(environment, null, name);
    }

    public static PersistentEntityStoreImpl newInstance(@NotNull final Environment environment) {
        return newInstance(environment, DEFAULT_NAME);
    }

    public static PersistentEntityStoreImpl newInstance(@NotNull final File dir) {
        return newInstance(Environments.newInstance(dir, new EnvironmentConfig()));
    }

    public static PersistentEntityStoreImpl newInstance(@NotNull final String dir) {
        return newInstance(new File(dir));
    }

    public static EnvironmentConfig adjustEnvironmentConfigForEntityStore(@NotNull final EnvironmentConfig ec) {
        if (ec.getEnvStoreGetCacheSize() == EnvironmentConfig.DEFAULT.getEnvStoreGetCacheSize()) {
            ec.setEnvStoreGetCacheSize(STORE_GET_CACHE_SIZE);
        }
        return ec.setEnvReadonlyEmptyStores(true);
    }
}