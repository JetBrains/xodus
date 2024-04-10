package jetbrains.exodus.query.metadata

import com.orientechnologies.orient.core.db.ODatabaseSession
import jetbrains.exodus.entitystore.PersistentEntityStore
import jetbrains.exodus.entitystore.orientdb.OPersistentEntityStore
import jetbrains.exodus.entitystore.orientdb.OVertexEntity
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.BINARY_BLOB_CLASS_NAME
import jetbrains.exodus.entitystore.orientdb.withSession


/**
 * This class is responsible for migrating data from Xodus to OrientDB.
 *
 * @param xodus The Xodus PersistentEntityStore instance.
 * @param orient The OrientDB OPersistentEntityStore instance.
 * @param entitiesPerTransaction The number of entities to be copied in a single transaction.
 */
class XodusToOrientDataMigrator(
    private val xodus: PersistentEntityStore,
    private val orient: OPersistentEntityStore,
    /*
    * How many entities should be copied in a single transaction
    * */
    private val entitiesPerTransaction: Int = 10
) {
    fun migrate() {
        orient.databaseProvider.withSession { oSession ->
            createVertexClassesIfAbsent(oSession)
            copyPropertiesAndBlobs(oSession)
            copyLinks()
        }
    }

    private fun createVertexClassesIfAbsent(oSession: ODatabaseSession) {
        // make sure all the vertex classes are created in OrientDB
        // classes can not be created in a transaction, so we have to create them before copying the data
        xodus.withReadonlyTx { xTx ->
            for (type in xTx.entityTypes) {
                oSession.getClass(type) ?: oSession.createVertexClass(type)
            }
        }

        oSession.getClass(BINARY_BLOB_CLASS_NAME) ?: oSession.createClass(BINARY_BLOB_CLASS_NAME)
    }

    private fun copyPropertiesAndBlobs(oSession: ODatabaseSession) {
        xodus.withReadonlyTx { xTx ->
            oSession.withCountingTx(entitiesPerTransaction) { countingTx ->
                for (type in xTx.entityTypes) {
                    for (xEntity in xTx.getAll(type)) {
                        val vertex = oSession.newVertex(type)
                        val oEntity = OVertexEntity(vertex, orient)
                        for (propName in xEntity.propertyNames) {
                            oEntity.setProperty(propName, xEntity.getProperty(propName) as Comparable<*>)
                        }
                        for (blobName in xEntity.blobNames) {
                            xEntity.getBlob(blobName)?.let { blobValue ->
                                oEntity.setBlob(blobName, blobValue)
                            }
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