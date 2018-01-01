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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

/**
 * {@code EntityStore} describes abstract named transactional
 * <a href="https://github.com/JetBrains/xodus/wiki/Entity-Stores">Entity Store</a>.
 *
 * @see StoreTransaction
 * @see PersistentEntityStore
 */
public interface EntityStore extends Closeable {

    /**
     * Returns unique human readable name of the (@code EntityStore}.
     *
     * @return name of the (@code EntityStore}
     */
    @NotNull
    String getName();

    /**
     * Returns location (path) of the {@code EntityStore}'s database files on storage device. This location as well as
     * {@linkplain #getName()} value is unique.
     *
     * @return location of the {@code EntityStore}'s database files
     */
    @NotNull
    String getLocation();

    /**
     * Starts new transaction which can be used to read and write data.
     *
     * @return new {@linkplain StoreTransaction} instance
     * @see StoreTransaction
     */
    @NotNull
    StoreTransaction beginTransaction();

    /**
     * Starts new exclusive transaction which can be used to read and write data. For given exclusive transaction,
     * it is guaranteed that no other transaction (except read-only ones) can be started on the {@code EntityStore}
     * before this one finishes.
     *
     * @return new {@linkplain StoreTransaction} instance
     * @see StoreTransaction
     */
    @NotNull
    StoreTransaction beginExclusiveTransaction();

    /**
     * Starts new transaction which can be used to only read data.
     *
     * @return new {@linkplain StoreTransaction} instance
     * @see StoreTransaction
     * @see StoreTransaction#isReadonly()
     */
    @NotNull
    StoreTransaction beginReadonlyTransaction();

    /**
     * @return {@linkplain StoreTransaction transaction} instance or {@code null} if no transaction is started in current thread
     * @see StoreTransaction
     */
    @Nullable
    StoreTransaction getCurrentTransaction();

    /**
     * Closes the {@code EntityStore}.
     */
    void close();
}
