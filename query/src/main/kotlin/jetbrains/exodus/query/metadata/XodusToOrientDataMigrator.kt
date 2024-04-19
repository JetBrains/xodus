package jetbrains.exodus.query.metadata

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.metadata.sequence.OSequence
import jetbrains.exodus.entitystore.EntityId
import jetbrains.exodus.entitystore.PersistentEntityStore
import jetbrains.exodus.entitystore.orientdb.*
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.LOCAL_ENTITY_ID_PROPERTY_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.BINARY_BLOB_CLASS_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.CLASS_ID_CUSTOM_PROPERTY_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.CLASS_ID_SEQUENCE_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.localEntityIdSequenceName

fun migrateDataFromXodusToOrientDb(
    xodus: PersistentEntityStore,
    orient: OPersistentEntityStore,
    /*
    * How many entities should be copied in a single transaction
    * */
    entitiesPerTransaction: Int = 10
) {
    val migrator = XodusToOrientDataMigrator(xodus, orient, entitiesPerTransaction)
    return migrator.migrate()
}

/**
 * This class is responsible for migrating data from Xodus to OrientDB.
 *
 * @param xodus The Xodus PersistentEntityStore instance.
 * @param orient The OrientDB OPersistentEntityStore instance.
 * @param entitiesPerTransaction The number of entities to be copied in a single transaction.
 */
internal class XodusToOrientDataMigrator(
    private val xodus: PersistentEntityStore,
    private val orient: OPersistentEntityStore,
    /*
    * How many entities should be copied in a single transaction
    * */
    private val entitiesPerTransaction: Int = 10
) {
    private val xEntityIdToOEntityId = HashMap<EntityId, EntityId>()

    fun migrate() {
        orient.databaseProvider.withSession { oSession ->
            createVertexClassesIfAbsent(oSession)
            val edgeClassesToCreate = copyPropertiesAndBlobs(oSession)
            createEdgeClassesIfAbsent(oSession, edgeClassesToCreate)
            copyLinks(oSession)
        }
    }

    private fun createVertexClassesIfAbsent(oSession: ODatabaseSession) {
        // make sure all the vertex classes are created in OrientDB
        // classes can not be created in a transaction, so we have to create them before copying the data
        var maxClassId = 0
        xodus.withReadonlyTx { xTx ->
            for (type in xTx.entityTypes) {
                val oClass = oSession.getClass(type) ?: oSession.createVertexClass(type)
                val classId = xodus.getEntityTypeId(type)

                oClass.setCustom(CLASS_ID_CUSTOM_PROPERTY_NAME, classId.toString())
                maxClassId = maxOf(maxClassId, classId)

                // create localEntityId property if absent
                if (oClass.getProperty(LOCAL_ENTITY_ID_PROPERTY_NAME) == null) {
                    oClass.createProperty(LOCAL_ENTITY_ID_PROPERTY_NAME, OType.LONG)
                }
            }
        }

        // create a sequence to generate classIds
        val sequences = oSession.metadata.sequenceLibrary
        require(sequences.getSequence(CLASS_ID_SEQUENCE_NAME) == null) { "$CLASS_ID_SEQUENCE_NAME is already created. It means that some data migration has happened to the target database before. Such a scenario is not supported." }
        oSession.createSequenceIfAbsent(CLASS_ID_SEQUENCE_NAME, maxClassId.toLong())

        oSession.getClass(BINARY_BLOB_CLASS_NAME) ?: oSession.createClass(BINARY_BLOB_CLASS_NAME)
    }

    private fun createEdgeClassesIfAbsent(oSession: ODatabaseSession, edgeClassesToCreate: Set<String>) {
        for (edgeClassName in edgeClassesToCreate) {
            oSession.getClass(edgeClassName) ?: oSession.createEdgeClass(edgeClassName)
        }
    }

    private fun copyPropertiesAndBlobs(oSession: ODatabaseSession): Set<String> {
        val edgeClassesToCreate = HashSet<String>()
        xodus.withReadonlyTx { xTx ->
            oSession.withCountingTx(entitiesPerTransaction) { countingTx ->
                for (type in xTx.entityTypes) {
                    var largestEntityId = 0L
                    for (xEntity in xTx.getAll(type)) {
                        val vertex = oSession.newVertex(type)
                        // copy localEntityId
                        val localEntityId = xEntity.id.localId
                        vertex.setProperty(LOCAL_ENTITY_ID_PROPERTY_NAME, localEntityId)
                        largestEntityId = maxOf(largestEntityId, localEntityId)

                        val oEntity = OVertexEntity(vertex, orient)
                        for (propName in xEntity.propertyNames) {
                            oEntity.setProperty(propName, xEntity.getProperty(propName) as Comparable<*>)
                        }
                        for (blobName in xEntity.blobNames) {
                            xEntity.getBlob(blobName)?.let { blobValue ->
                                oEntity.setBlob(blobName, blobValue)
                            }
                        }
                        xEntityIdToOEntityId[xEntity.id] = oEntity.id
                        countingTx.increment()

                        edgeClassesToCreate.addAll(xEntity.linkNames)
                    }

                    // create a sequence to generate localEntityIds for the class
                    oSession.createSequenceIfAbsent(localEntityIdSequenceName(type), largestEntityId)
                }
            }
        }
        return edgeClassesToCreate
    }

    private fun ODatabaseSession.createSequenceIfAbsent(sequenceName: String, startingFrom: Long) {
        val sequences = metadata.sequenceLibrary
        if (sequences.getSequence(sequenceName) == null) {
            val params = OSequence.CreateParams()
            params.start = startingFrom
            sequences.createSequence(sequenceName, OSequence.SEQUENCE_TYPE.ORDERED, params)
        }
    }

    private fun copyLinks(oSession: ODatabaseSession) {
        xodus.withReadonlyTx { xTx ->
            oSession.withCountingTx(entitiesPerTransaction) { countingTx ->
                for (type in xTx.entityTypes) {
                    for (xEntity in xTx.getAll(type)) {
                        val oEntityId = xEntityIdToOEntityId.getValue(xEntity.id)
                        val oEntity = orient.getEntity(oEntityId)

                        var copiedSomeLinks = false
                        for (linkName in xEntity.linkNames) {
                            for (xTargetEntity in xEntity.getLinks(linkName)) {
                                val oTargetId = xEntityIdToOEntityId.getValue(xTargetEntity.id)
                                oEntity.addLink(linkName, oTargetId)
                                copiedSomeLinks = true
                            }
                        }
                        if (copiedSomeLinks) {
                            countingTx.increment()
                        }
                    }
                }
            }
        }
    }
}