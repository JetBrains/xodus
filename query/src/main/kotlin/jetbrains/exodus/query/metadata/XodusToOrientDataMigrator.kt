package jetbrains.exodus.query.metadata

import com.orientechnologies.orient.core.db.ODatabaseSession
import jetbrains.exodus.entitystore.PersistentEntityStore
import jetbrains.exodus.entitystore.orientdb.OPersistentEntityStore
import jetbrains.exodus.entitystore.orientdb.OVertexEntity


/**
 * This class is responsible for migrating data from Xodus to OrientDB.
 *
 * @param xodus The Xodus PersistentEntityStore instance.
 * @param orient The OrientDB OPersistentEntityStore instance.
 * @param oSession The OrientDB ODatabaseSession instance.
 * @param entitiesPerTransaction The number of entities to be copied in a single transaction.
 */
class XodusToOrientDataMigrator(
    private val xodus: PersistentEntityStore,
    private val orient: OPersistentEntityStore,
    private val oSession: ODatabaseSession,
    /*
    * How many entities should be copied in a single transaction
    * */
    private val entitiesPerTransaction: Int = 10
) {
    fun migrate() {
        createVertexClassesIfAbsent()
        copyPropertiesAndBlobs()
        copyLinks()
    }

    private fun createVertexClassesIfAbsent() {
        // make sure all the vertex classes are created in OrientDB
        // classes can not be created in a transaction, so we have to create them before copying the data
        xodus.withReadonlyTx { xTx ->
            for (type in xTx.entityTypes) {
                oSession.getClass(type) ?: oSession.createVertexClass(type)
            }
        }
    }

    private fun copyPropertiesAndBlobs() {
        xodus.withReadonlyTx { xTx ->
            oSession.withCountingTx(entitiesPerTransaction) { countingTx ->
                for (type in xTx.entityTypes) {
                    for (entity in xTx.getAll(type)) {
                        val vertex = oSession.newVertex(type)
                        val oEntity = OVertexEntity(vertex, orient)
                        for (prop in entity.propertyNames) {
                            oEntity.setProperty(prop, entity.getProperty(prop) as Comparable<*>)
                        }
                        countingTx.increment()
                    }
                }
            }
        }
    }

    private fun copyLinks() {

    }
}