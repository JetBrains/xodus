package jetbrains.exodus.query.metadata

import com.orientechnologies.orient.core.db.ODatabaseSession


/**
 * A class that provides functionality for commiting a transaction every X changes.
 *
 * For example, you copy 1B entities, you do not want to copy them in the scope of a single transaction,
 * because when the transaction fails at the last entity, you have to start copying from the beginning.
 *
 * If you use a counting transaction and set it commit every 100 entities. You just copy entity by entity,
 * increment() the transaction, and it will make sure to commit when necessary.
 *
 * @property oSession The underlying ODatabaseSession that this CountingOTransaction operates on.
 * @property commitEvery The number of changes increments before a commit is triggered.
 */
class CountingOTransaction(
    private val oSession: ODatabaseSession,
    private val commitEvery: Int
) {
    private var counter = 0

    fun increment() {
        counter++
        if (counter == commitEvery) {
            oSession.commit()
            oSession.begin()
            counter = 0
        }
    }

    fun begin() {
        require(oSession.transaction == null || !oSession.transaction.isActive)
        oSession.begin()
    }

    fun commit() {
        oSession.transaction?.let { tx ->
            if (tx.isActive) {
                oSession.commit()
            }
        }
    }

    fun rollback() {
        oSession.transaction?.let { tx ->
            if (tx.isActive) {
                oSession.rollback()
            }
        }
    }
}

fun <R> ODatabaseSession.withCountingTx(commitEvery: Int, block: (CountingOTransaction) -> R): R {
    val tx = CountingOTransaction(this, commitEvery)
    tx.begin()
    try {
        val result = block(tx)
        tx.commit()
        return result
    } catch(e: Throwable) {
        tx.rollback()
        throw e
    }
}