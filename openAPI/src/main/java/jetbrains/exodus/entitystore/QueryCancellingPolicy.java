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

/**
 * Use {@linkplain StoreTransaction#setQueryCancellingPolicy(QueryCancellingPolicy)} to set an instance of
 * {@code QueryCancellingPolicy} to be able to interrupt iteration over {@linkplain EntityIterable} instances
 * associated with the {@code StoreTransaction}. The most common use case is setting timeout for the
 * {@code StoreTransaction} instance.
 *
 * <p>{@linkplain PersistentEntityStore} implementation tries to to check {@code QueryCancellingPolicy} in a most
 * smart way in order to affect execution performance as little as possible.
 *
 * @see StoreTransaction
 * @see StoreTransaction#setQueryCancellingPolicy(QueryCancellingPolicy)
 * @see EntityIterable
 * @see EntityIterable#getTransaction()
 */
public interface QueryCancellingPolicy {
    QueryCancellingPolicy NONE = new QueryCancellingPolicy() {
        @Override
        public boolean needToCancel() {
            return false;
        }

        @Override
        public void doCancel() {
        }
    };


    /**
     * @return {@code true} when it's time to cancel iteration over {@linkplain EntityIterable} instances which were
     * created in the {@linkplain StoreTransaction} supplied with the {@code QueryCancellingPolicy} instance
     */
    boolean needToCancel();

    /**
     * Cancels iteration over {@linkplain EntityIterable} instances which were created in the
     * {@linkplain StoreTransaction} supplied with the {@code QueryCancellingPolicy} instance. Usually it throws an
     * exception which application code can handle.
     */
    void doCancel();
}
