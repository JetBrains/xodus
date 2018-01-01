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

import jetbrains.exodus.env.TransactionalExecutable;
import org.jetbrains.annotations.NotNull;

/**
 * A function that can pe passed to the {@linkplain PersistentEntityStore#executeInTransaction(StoreTransactionalExecutable)}
 * or {@linkplain PersistentEntityStore#executeInReadonlyTransaction(StoreTransactionalExecutable)}to be executed
 * within a {@linkplain StoreTransaction transaction}.
 *
 * @see StoreTransaction
 * @see PersistentEntityStore#executeInTransaction(StoreTransactionalExecutable)
 * @see PersistentEntityStore#executeInReadonlyTransaction(StoreTransactionalExecutable)
 * @see StoreTransactionalComputable
 * @see TransactionalExecutable
 */
public interface StoreTransactionalExecutable {

    void execute(@NotNull final StoreTransaction txn);
}
