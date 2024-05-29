/*
 * Copyright ${inceptionYear} - ${year} ${owner}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.query.metadata

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.metadata.sequence.OSequence
import com.orientechnologies.orient.core.record.OVertex
import jetbrains.exodus.bindings.ComparableSet
import jetbrains.exodus.entitystore.EntityId
import jetbrains.exodus.entitystore.EntityRemovedInDatabaseException
import jetbrains.exodus.entitystore.PersistentEntityStore
import jetbrains.exodus.entitystore.orientdb.*
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.BINARY_BLOB_CLASS_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.CLASS_ID_CUSTOM_PROPERTY_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.CLASS_ID_SEQUENCE_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.LOCAL_ENTITY_ID_PROPERTY_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.localEntityIdSequenceName
import mu.KotlinLogging

private val log = KotlinLogging.logger { }

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
    private val entitiesPerTransaction: Int = 10,
    private val printProgressAtLeastOnceIn: Int = 5_000
) {
    private val xEntityIdToOEntityId = HashMap<EntityId, EntityId>()

    fun migrate() {
        log.info { "Starting Xodus -> OrientDB data migration" }
        createVertexClassesIfAbsent()
        val edgeClassesToCreate = copyPropertiesAndBlobs()
        createEdgeClassesIfAbsent(edgeClassesToCreate)
        copyLinks()
    }

    private fun createVertexClassesIfAbsent() {
        // make sure all the vertex classes are created in OrientDB
        // classes can not be created in a transaction, so we have to create them before copying the data
        log.info { "1. Copy entity types" }
        var maxClassId = 0
        orient.databaseProvider.withSession { oSession ->
            xodus.withReadonlyTx { xTx ->
                val entityTypes = xTx.entityTypes.toSet()
                log.info { "${entityTypes.size} entity types to copy" }
                entityTypes.forEachIndexed { i, type ->
                    log.info { "$i $type is being copied" }
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
            log.info { "All the types have been copied" }
        }
    }

    private fun copyPropertiesAndBlobs(): Set<String> {
        log.info { "2. Copy entities, their simple properties and blobs" }
        val edgeClassesToCreate = HashSet<String>()
        xodus.withReadonlyTx { xTx ->
            orient.withCountingTx(entitiesPerTransaction) { countingTx ->
                var totalEntities = 0L
                var totalProperties = 0
                var totalBlobs = 0
                val entityTypes = xTx.entityTypes.toSet()
                entityTypes.forEachIndexed { typeIdx, type ->
                    var largestEntityId = 0L
                    val xEntities = xTx.getAll(type)
                    xodus.getEntityTypeId(type)
                    log.info { "$typeIdx $type ${xEntities.size()} entities to copy" }
                    var properties = 0
                    var blobs = 0
                    var lastProgressPrintedAt = System.currentTimeMillis()
                    xEntities.forEachIndexed { xEntityIdx, xEntity ->
                        val vertex = countingTx.session.newVertex(type)
                        // copy localEntityId
                        val localEntityId = xEntity.id.localId
                        vertex.setProperty(LOCAL_ENTITY_ID_PROPERTY_NAME, localEntityId)
                        largestEntityId = maxOf(largestEntityId, localEntityId)
                        vertex.save<OVertex>()

                        val oEntity = OVertexEntity(vertex, orient)
                        for (propName in xEntity.propertyNames) {
                            val propValue = xEntity.getProperty(propName)
                            if (propValue is ComparableSet<*>) {
                                // todo ignore for now, how the bug is fixed, delete this if
                            } else {
                                oEntity.setProperty(propName, propValue as Comparable<*>)
                                properties++
                            }
                        }
                        for (blobName in xEntity.blobNames) {
                            xEntity.getBlob(blobName)?.let { blobValue ->
                                oEntity.setBlob(blobName, blobValue)
                                blobs++
                            }
                        }
                        xEntityIdToOEntityId[xEntity.id] = oEntity.id
                        countingTx.increment()

                        if (System.currentTimeMillis() - lastProgressPrintedAt > printProgressAtLeastOnceIn) {
                            log.info { "$typeIdx $type current entity: $xEntityIdx, properties processed: $properties, blobs copied: $blobs" }
                            lastProgressPrintedAt = System.currentTimeMillis()
                        }

                        edgeClassesToCreate.addAll(xEntity.linkNames)
                    }

                    // create a sequence to generate localEntityIds for the class
                    countingTx.session.createSequenceIfAbsent(localEntityIdSequenceName(type), largestEntityId)
                    log.info { "$typeIdx $type entities copied: ${xEntities.size()}, properties copied: $properties, blobs copied: $blobs" }
                    totalEntities += xEntities.size()
                    totalProperties += properties
                    totalBlobs += blobs
                }
                log.info { "Entities have been copied. Entity types: ${entityTypes.size}, entities copied: $totalEntities, properties copied: $totalProperties, blobs copied: $totalBlobs" }
            }
        }
        return edgeClassesToCreate
    }

    private fun createEdgeClassesIfAbsent(edgeClassesToCreate: Set<String>) {
        log.info { "3. Create edge classes" }
        log.info { "${edgeClassesToCreate.size} edge classes to create" }
        orient.databaseProvider.withSession { oSession ->
            edgeClassesToCreate.forEachIndexed { i, edgeClassName ->
                log.info { "$i $edgeClassName ${edgeClassName.asEdgeClass} is being copied" }
                oSession.getClass(edgeClassName.asEdgeClass) ?: oSession.createEdgeClass(edgeClassName.asEdgeClass)
            }
        }
        log.info { "${edgeClassesToCreate.size} edge classes have been created" }
    }

    private fun copyLinks() {
        log.info { "4. Copy links" }
        xodus.withReadonlyTx { xTx ->
            orient.withCountingTx(entitiesPerTransaction) { countingTx ->
                val entityTypes = xTx.entityTypes.toSet()
                log.info { "${entityTypes.size} entity types to copy links" }
                var totalEntities = 0L
                var totalLinksProcessed = 0
                var totalLinksCopied = 0
                entityTypes.forEachIndexed { typeIdx, type ->
                    var linksProcessed = 0
                    var linksCopied = 0
                    val xEntities = xTx.getAll(type)
                    log.info { "$typeIdx $type ${xEntities.size()} entities to copy links" }
                    var lastProgressPrintedAt = System.currentTimeMillis()
                    xEntities.forEachIndexed { xEntityIdx, xEntity ->
                        val oEntityId = xEntityIdToOEntityId.getValue(xEntity.id)
                        val oEntity = orient.getEntity(oEntityId)

                        var copiedSomeLinks = false
                        for (linkName in xEntity.linkNames) {
                            for (xTargetEntity in xEntity.getLinks(linkName)) {
                                linksProcessed++
                                try {
                                    xTx.getEntity(xTargetEntity.id)
                                    val oTargetId = xEntityIdToOEntityId.getValue(xTargetEntity.id)
                                    oEntity.addLink(linkName, oTargetId)
                                    copiedSomeLinks = true
                                    linksCopied++
                                } catch (e: EntityRemovedInDatabaseException) {
                                    // ignore
                                }
                                if (System.currentTimeMillis() - lastProgressPrintedAt > printProgressAtLeastOnceIn) {
                                    log.info { "$typeIdx $type current entity: $xEntityIdx, links processed: $linksProcessed, links copied: $linksCopied" }
                                    lastProgressPrintedAt = System.currentTimeMillis()
                                }
                            }
                        }
                        if (copiedSomeLinks) {
                            countingTx.increment()
                        }
                    }
                    log.info { "$typeIdx $type entities processed: ${xEntities.size()}, links processed: $linksProcessed, links copied: $linksCopied" }
                    totalEntities += xEntities.size()
                    totalLinksProcessed += linksProcessed
                    totalLinksCopied += linksCopied
                }
                log.info { "Links have been copied. Entity types: ${entityTypes.size}, entities processed: $totalEntities, links processed: $totalLinksProcessed, links copied: $totalLinksCopied" }
            }
        }
    }

    private fun ODatabaseSession.createSequenceIfAbsent(sequenceName: String, startingFrom: Long) {
        val sequences = metadata.sequenceLibrary
        if (sequences.getSequence(sequenceName) == null) {
            val params = OSequence.CreateParams()
            params.start = startingFrom
            sequences.createSequence(sequenceName, OSequence.SEQUENCE_TYPE.ORDERED, params)
        }
    }

}
