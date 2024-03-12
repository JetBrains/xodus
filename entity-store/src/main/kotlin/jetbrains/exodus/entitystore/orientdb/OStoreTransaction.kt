package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import jetbrains.exodus.entitystore.StoreTransaction

interface OStoreTransaction : StoreTransaction {

    fun activeSession(): ODatabaseDocument
}