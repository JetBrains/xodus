/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
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
package jetbrains.exodus.env

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.tree.TreeMetaInfo

open class ContextualStoreImpl(private val environment: ContextualEnvironmentImpl, name: String, metaInfo: TreeMetaInfo) :
    StoreImpl(
        environment, name, metaInfo
    ), ContextualStore {
    override fun getEnvironment(): ContextualEnvironmentImpl {
        return environment
    }

    override fun get(key: ByteIterable): ByteIterable? {
        return get(environment.getAndCheckCurrentTransaction(), key)
    }

    override fun exists(key: ByteIterable, value: ByteIterable): Boolean {
        return exists(environment.getAndCheckCurrentTransaction(), key, value)
    }

    override fun put(key: ByteIterable, value: ByteIterable): Boolean {
        return put(environment.getAndCheckCurrentTransaction(), key, value)
    }

    override fun putRight(key: ByteIterable, value: ByteIterable) {
        putRight(environment.getAndCheckCurrentTransaction(), key, value)
    }

    override fun add(key: ByteIterable, value: ByteIterable): Boolean {
        return add(environment.getAndCheckCurrentTransaction(), key, value)
    }

    override fun delete(key: ByteIterable): Boolean {
        return delete(environment.getAndCheckCurrentTransaction(), key)
    }

    override fun count(): Long {
        return count(environment.getAndCheckCurrentTransaction())
    }

    override fun openCursor(): Cursor {
        return openCursor(environment.getAndCheckCurrentTransaction())
    }
}
