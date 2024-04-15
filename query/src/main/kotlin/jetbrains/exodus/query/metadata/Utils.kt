package jetbrains.exodus.query.metadata

import com.orientechnologies.orient.core.db.ODatabaseSession
import jetbrains.exodus.entitystore.PersistentEntityStore
import jetbrains.exodus.entitystore.StoreTransaction

fun <R> PersistentEntityStore.withReadonlyTx(block: (StoreTransaction) -> R): R {
    val tx = this.beginReadonlyTransaction()
    try {
        val result = block(tx)
        tx.abort()
        return result
    } catch(e: Throwable) {
        if (!tx.isFinished) {
            tx.abort()
        }
        throw e
    }
}

fun <R> ODatabaseSession.withTx(block: (ODatabaseSession) -> R): R {
    this.begin()
    try {
        val result = block(this)
        this.commit()
        return result
    } catch(e: Throwable) {
        this.rollback()
        throw e
    }
}