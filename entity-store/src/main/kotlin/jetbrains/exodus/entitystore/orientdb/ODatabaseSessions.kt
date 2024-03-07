package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.db.record.OIdentifiable
import com.orientechnologies.orient.core.record.OVertex
import jetbrains.exodus.entitystore.Entity

fun ODatabaseDocument.getEntity(oid: OIdentifiable): Entity {
    val record = this.getRecord<OVertex>(oid)
    return OVertexEntity(record)
}