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

import jetbrains.exodus.env.TransactionalComputable;
import org.jetbrains.annotations.NotNull;

/**
 * A function that can pe passed to the {@linkplain PersistentEntityStore#computeInTransaction(StoreTransactionalComputable)}
 * or {@linkplain PersistentEntityStore#computeInReadonlyTransaction(StoreTransactionalComputable)} to be executed and
 * return result within a {@linkplain StoreTransaction transaction}.
 *
 * @param <T> type of returned result
 * @see StoreTransaction
 * @see PersistentEntityStore#computeInTransaction(StoreTransactionalComputable) `
 * @see PersistentEntityStore#computeInReadonlyTransaction(StoreTransactionalComputable)
 * @see StoreTransactionalExecutable
 * @see TransactionalComputable
 */
public interface StoreTransactionalComputable<T> {

    T compute(@NotNull final StoreTransaction txn);
}
