/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.entitystore;

/**
 * {@code Sequence} allows to get unique successive non-negative long numbers. Sequences are named,
 * you can request a sequence by name using methods {@linkplain StoreTransaction#getSequence(String)} or
 * {@linkplain StoreTransaction#getSequence(String, long)}. Sequences are persistent, any
 * flushed or committed {@linkplain StoreTransaction transaction} saves all dirty (mutated) sequences which were
 * requested by transactions created against current {@linkplain PersistentEntityStore}.
 *
 * <p>{@linkplain PersistentEntityStore} implementation uses {@code Sequences} to generate {@linkplain EntityId} instances.
 *
 * @see StoreTransaction#getSequence(String)
 * @see StoreTransaction#getSequence(String, long)
 */
public interface Sequence {

    /**
     * Returns next non-negative number. For a {@code Sequence} created by method
     * {@linkplain StoreTransaction#getSequence(String)}, it starts with {@code 0}.
     * For a {@code Sequence} created by method
     * {@linkplain StoreTransaction#getSequence(String, long)}, it starts with specified initial value.
     *
     * @return next number
     */
    long increment();

    /**
     * Returns current number obtained by last call to {@linkplain #increment()}. If {@linkplain #increment()} was
     * never called it returns {@code -1}.
     *
     * @return current number obtained by last call to {@linkplain #increment()} or {@code -1}
     */
    long get();

    /**
     * Sets current number. Next call to {@linkplain #increment()} will return {@code l + 1}.
     *
     * @param l current number
     */
    void set(final long l);
}
