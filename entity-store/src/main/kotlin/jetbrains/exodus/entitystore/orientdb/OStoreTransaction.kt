package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.tx.OTransaction
import jetbrains.exodus.entitystore.StoreTransaction

interface OStoreTransaction : StoreTransaction {

    val activeSession: ODatabaseDocument
    val oTransaction: OTransaction
}