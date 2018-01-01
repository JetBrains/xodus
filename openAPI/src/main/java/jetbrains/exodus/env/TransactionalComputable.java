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
 * A function that can pe passed to the {@linkplain Environment#computeInTransaction(TransactionalComputable)},
 * {@linkplain Environment#computeInReadonlyTransaction(TransactionalComputable)} or
 * {@linkplain Environment#computeInExclusiveTransaction(TransactionalComputable)} to be executed and return result
 * within a {@linkplain Transaction transaction}.
 *
 * @param <T> type of returned result
 * @see Transaction
 * @see Environment#computeInTransaction(TransactionalComputable)
 * @see Environment#computeInReadonlyTransaction(TransactionalComputable)
 * @see Environment#computeInExclusiveTransaction(TransactionalComputable)
 * @see TransactionalExecutable
 */
public interface TransactionalComputable<T> {

    T compute(@NotNull final Transaction txn);
}
