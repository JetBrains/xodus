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

class ReadonlyTransaction : TransactionBase {
    private val beginHook: Runnable?

    constructor(env: EnvironmentImpl, exclusive: Boolean, beginHook: Runnable?) : super(env, exclusive) {
        this.beginHook = getWrappedBeginHook(beginHook)
        env.holdNewestSnapshotBy(this, false)
    }

    /**
     * Constructor for creating new snapshot transaction.
     */
    internal constructor(origin: TransactionBase) : super(origin.environment, false) {
        beginHook = null
        setMetaTree(origin.getMetaTree())

        val env = environment
        env.registerTransaction(this)
    }

    override fun beginHook(): Runnable? = beginHook

    override fun setCommitHook(hook: Runnable?) {
        throw ReadonlyTransactionException()
    }

    override fun storeRemoved(store: StoreImpl) {
        throw ReadonlyTransactionException()
    }

    override fun isIdempotent(): Boolean {
        return true
    }

    override fun abort() {
        checkIsFinished()
        environment.finishTransaction(this)
    }

    override fun commit(): Boolean {
        if (!isExclusive) {
            throw ReadonlyTransactionException()
        }
        return true
    }

    override fun flush(): Boolean {
        throw ReadonlyTransactionException()
    }

    override fun revert() {
        throw ReadonlyTransactionException()
    }

    override fun isReadonly(): Boolean {
        return true
    }
}
