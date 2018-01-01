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

/**
 * A function that can pe passed to the {@linkplain Environment#executeInTransaction(TransactionalExecutable)},
 * {@linkplain Environment#executeInReadonlyTransaction(TransactionalExecutable)} or
 * {@linkplain Environment#executeInExclusiveTransaction(TransactionalExecutable)} to be executed
 * within a {@linkplain Transaction transaction}.
 *
 * @see Transaction
 * @see Environment#executeInTransaction(TransactionalExecutable)
 * @see Environment#executeInReadonlyTransaction(TransactionalExecutable)
 * @see Environment#executeInExclusiveTransaction(TransactionalExecutable)
 * @see TransactionalComputable
 */
public interface TransactionalExecutable {

    void execute(@NotNull final Transaction txn);
}
