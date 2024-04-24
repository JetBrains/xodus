package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.record.OVertex
import jetbrains.exodus.entitystore.PersistentEntityStore

class OReadonlyVertexEntity(val txn: OStoreTransaction, id: OEntityId) : OVertexEntity(
    txn.activeSession.load<OVertex>(id.asOId()), txn.store as PersistentEntityStore
) {
    override fun assertWritable() {
        super.assertWritable()
        throw IllegalArgumentException("Can't update readonly entity!")
    }
}

