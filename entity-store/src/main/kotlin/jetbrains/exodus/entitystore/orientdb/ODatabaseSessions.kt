package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.db.record.OIdentifiable
import com.orientechnologies.orient.core.record.OVertex
import com.orientechnologies.orient.core.sql.executor.OResultSet
import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.orientdb.query.OQuery

fun ODatabaseDocument.getVertexEntity(oid: OIdentifiable): Entity {
    val record = this.getRecord<OVertex>(oid)
    return OVertexEntity(record)
}

fun ODatabaseDocument.query(query: OQuery): OResultSet {
    return query(query.sql(), *query.params().toTypedArray())
}