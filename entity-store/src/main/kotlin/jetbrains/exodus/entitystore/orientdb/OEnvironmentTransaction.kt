package jetbrains.exodus.entitystore.orientdb

import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.Transaction

class OEnvironmentTransaction(
    private val environment: Environment,
    private val txn: OStoreTransaction
) : Transaction {

    private val userObjects = HashMap<Any, Any>()
    private val start = System.currentTimeMillis()
    private var commitHook: Runnable? = null

    override fun isIdempotent(): Boolean {
        return txn.isIdempotent
    }

    override fun abort() {
        txn.abort()
    }

    override fun commit(): Boolean {
        commitHook?.run()
        return txn.commit()
    }

    override fun flush(): Boolean {
        return txn.flush()
    }

    override fun revert() {
        txn.revert()
    }

    override fun getSnapshot(): Transaction {
        return this
    }

    override fun getSnapshot(beginHook: Runnable?): Transaction {
        beginHook?.run()
        return this
    }

    override fun getReadonlySnapshot(): Transaction {
        return this
    }

    override fun getEnvironment(): Environment {
        return environment
    }

    override fun setCommitHook(hook: Runnable?) {
        this.commitHook = hook
    }

    override fun getStartTime(): Long {
        return start
    }

    override fun getSnapshotId(): Long {
        return txn.activeSession.transaction.id.toLong()
    }

    override fun isReadonly(): Boolean {
        return txn.isReadonly
    }

    override fun isExclusive(): Boolean {
        return false
    }

    override fun isFinished(): Boolean {
        return txn.isFinished
    }

    override fun getUserObject(key: Any): Any? {
        return userObjects[key]
    }

    override fun setUserObject(key: Any, value: Any) {
        userObjects[key] = value
    }
}
