package jetbrains.exodus.entitystore.orientdb.query

import com.orientechnologies.orient.core.db.document.ODatabaseDocument

class OCountSelect(
    val source: OSelect,
) : OQuery {

    override fun sql() = "SELECT count(*) as count FROM (${source.sql()})"
    override fun params() = source.params()

    fun count(session: ODatabaseDocument? = null): Long = execute(session).next().getProperty<Long>("count")
}