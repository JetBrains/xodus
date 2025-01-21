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


import com.jetbrains.youtrack.db.api.schema.PropertyType
import jetbrains.exodus.bindings.ComparableSet
import jetbrains.exodus.entitystore.EntityId
import jetbrains.exodus.entitystore.EntityRemovedInDatabaseException
import jetbrains.exodus.entitystore.PersistentEntityStore
import jetbrains.exodus.entitystore.youtrackdb.*
import jetbrains.exodus.entitystore.youtrackdb.YTDBVertexEntity.Companion.CLASS_ID_CUSTOM_PROPERTY_NAME
import jetbrains.exodus.entitystore.youtrackdb.YTDBVertexEntity.Companion.CLASS_ID_SEQUENCE_NAME
import jetbrains.exodus.entitystore.youtrackdb.YTDBVertexEntity.Companion.LOCAL_ENTITY_ID_PROPERTY_NAME
import jetbrains.exodus.entitystore.youtrackdb.YTDBVertexEntity.Companion.localEntityIdSequenceName
import mu.KotlinLogging
import kotlin.time.Duration
import kotlin.time.measureTime
import kotlin.time.measureTimedValue

private val log = KotlinLogging.logger { }

fun migrateDataFromXodusToOrientDb(
    xodus: PersistentEntityStore,
    orient: YTDBPersistentEntityStore,
    orientProvider: YTDBDatabaseProvider,
    schemaBuddy: YTDBSchemaBuddy,
    /*
    * How many entities should be copied in a single transaction
    * */
    entitiesPerTransaction: Int = 10
): XodusToOrientMigrationStats {
    val migrator = XodusToOrientDataMigrator(
        xodus,
        orient,
        orientProvider,
        schemaBuddy,
        entitiesPerTransaction
    )
    return migrator.migrate()
}

data class XodusToOrientMigrationStats(
    val entityClasses: Int,
    val createEntityClassesDuration: Duration,
    val entities: Long,
    val properties: Long,
    val blobs: Long,
    val copyEntitiesPropertiesAndBlobsTransactions: Long,
    val copyEntitiesPropertiesAndBlobsDuration: Duration,
    val createEntitiesDuration: Duration,
    val copyPropertiesDuration: Duration,
    val copyBlobsDuration: Duration,
    val commitEntitiesPropertiesAndBlobsDuration: Duration,

    val edgeClasses: Int,
    val createEdgeClassesDuration: Duration,
    val processedLinks: Long,
    val copiedLinks: Long,
    val copyLinksTransactions: Long,
    val copyLinksTotalDuration: Duration,
    val copyLinksDuration: Duration,
    val commitLinksDuration: Duration,

    val xEntityIdToOEntityId: Map<EntityId, EntityId>
)

/**
 * This class is responsible for migrating data from Xodus to OrientDB.
 *
 * @param xodus The Xodus PersistentEntityStore instance.
 * @param orient The OrientDB OPersistentEntityStore instance.
 * @param entitiesPerTransaction The number of entities to be copied in a single transaction.
 */
internal class XodusToOrientDataMigrator(
    private val xodus: PersistentEntityStore,
    private val orient: YTDBPersistentEntityStore,
    private val orientProvider: YTDBDatabaseProvider,
    private val schemaBuddy: YTDBSchemaBuddy,
    /*
    * How many entities should be copied in a single transaction
    * */
    private val entitiesPerTransaction: Int = 10,
    private val printProgressAtLeastOnceIn: Int = 5_000
) {
    private val xEntityIdToOEntityId = HashMap<EntityId, EntityId>()

    private var entityClassesCount = 0
    private var edgeClassesCount = 0

    private var totalEntities = 0L
    private var totalProperties = 0L
    private var totalBlobs = 0L
    private var createEntitiesDuration = Duration.ZERO
    private var copyPropertiesDuration = Duration.ZERO
    private var copyBlobsDuration = Duration.ZERO
    private var commitEntitiesDuration = Duration.ZERO

    private var totalLinksProcessed = 0L
    private var totalLinksCopied = 0L
    private var copyLinksDuration = Duration.ZERO
    private var commitLinksDuration = Duration.ZERO

    private var copyEntitiesPropertiesAndBlobsTransactions = 0L
    private var copyLinksTransactions = 0L

    fun migrate(): XodusToOrientMigrationStats {
        log.info { "Starting Xodus -> OrientDB data migration" }
        val createVertexClassesDuration = measureTime {
            createVertexClassesIfAbsent()
        }
        val (edgeClassesToCreate, copyPropertiesAndBlobsDuration) = measureTimedValue {
            copyPropertiesAndBlobs()
        }
        val createEdgeClassesDuration = measureTime {
            createEdgeClassesIfAbsent(edgeClassesToCreate)
        }
        edgeClassesCount = edgeClassesToCreate.size
        val copyLinksTotalDuration = measureTime {
            copyLinks()
        }
        return XodusToOrientMigrationStats(
            entityClasses = entityClassesCount,
            createEntityClassesDuration = createVertexClassesDuration,

            entities = totalEntities,
            properties = totalProperties,
            blobs = totalBlobs,
            copyEntitiesPropertiesAndBlobsTransactions = copyEntitiesPropertiesAndBlobsTransactions,
            copyEntitiesPropertiesAndBlobsDuration = copyPropertiesAndBlobsDuration,
            createEntitiesDuration = createEntitiesDuration,
            copyPropertiesDuration = copyPropertiesDuration,
            copyBlobsDuration = copyBlobsDuration,
            commitEntitiesPropertiesAndBlobsDuration = commitEntitiesDuration,

            edgeClasses = edgeClassesCount,
            createEdgeClassesDuration = createEdgeClassesDuration,

            processedLinks = totalLinksProcessed,
            copiedLinks = totalLinksCopied,
            copyLinksTransactions = copyLinksTransactions,
            copyLinksTotalDuration = copyLinksTotalDuration,
            copyLinksDuration = copyLinksDuration,
            commitLinksDuration = commitLinksDuration,

            xEntityIdToOEntityId = xEntityIdToOEntityId
        )
    }

    private fun createVertexClassesIfAbsent() {
        // make sure all the vertex classes are created in OrientDB
        // classes can not be created in a transaction, so we have to create them before copying the data
        log.info { "1. Copy entity types" }
        var maxClassId = 0
        orientProvider.withSession { oSession ->
            xodus.withReadonlyTx { xTx ->
                val entityTypes = xTx.entityTypes.toSet()
                log.info { "${entityTypes.size} entity types to copy" }
                entityTypes.forEachIndexed { i, type ->
                    log.info { "$i $type is being copied" }
                    val oClass = oSession.getClass(type) ?: oSession.createVertexClass(type)
                    val classId = xodus.getEntityTypeId(type)

                    oClass.setCustom(oSession, CLASS_ID_CUSTOM_PROPERTY_NAME, classId.toString())
                    maxClassId = maxOf(maxClassId, classId)

                    // create localEntityId property if absent
                    if (oClass.getProperty(LOCAL_ENTITY_ID_PROPERTY_NAME) == null) {
                        oClass.createProperty(oSession, LOCAL_ENTITY_ID_PROPERTY_NAME,PropertyType.LONG)
                    }
                }
                entityClassesCount = entityTypes.size
            }

            // create a sequence to generate classIds
            check(
                schemaBuddy.getSequenceOrNull(
                    oSession,
                    CLASS_ID_SEQUENCE_NAME
                ) == null
            ) { "$CLASS_ID_SEQUENCE_NAME is already created. It means that some data migration has happened to the target database before. Such a scenario is not supported." }
            oSession.createClassIdSequenceIfAbsent(maxClassId.toLong())

            log.info { "All the types have been copied" }
        }
    }

    private fun copyPropertiesAndBlobs(): Set<String> {
        log.info { "2. Copy entities, their simple properties and blobs" }
        val edgeClassesToCreate = HashSet<String>()
        val sequencesToCreate =
            mutableListOf<Pair<String, Long>>() // sequenceName, largestExistingId
        xodus.withReadonlyTx { xTx ->
            orient.withCountingTx(entitiesPerTransaction) { countingTx ->
                val entityTypes = xTx.entityTypes.toSet()
                val entityTypesCount = entityTypes.size
                entityTypes.forEachIndexed { typeIdx, type ->
                    var largestEntityId = 0L
                    val xEntities = xTx.getAll(type)
                    val xEntitiesCount = xEntities.size()
                    log.info { "$typeIdx/${entityTypesCount} $type $xEntitiesCount entities to copy" }
                    var properties = 0
                    var blobs = 0
                    var lastProgressPrintedAt = System.currentTimeMillis()
                    xEntities.forEachIndexed { xEntityIdx, xEntity ->
                        val (oEntity, createVertexDuration) = measureTimedValue {
                            val localEntityId = xEntity.id.localId
                            largestEntityId = maxOf(largestEntityId, localEntityId)

                            countingTx.newVertex(type, localEntityId)
                        }
                        createEntitiesDuration += createVertexDuration

                        copyPropertiesDuration += measureTime {
                            for (propName in xEntity.propertyNames) {
                                val propValue = xEntity.getProperty(propName)
                                val comparableValue = if (propValue is ComparableSet<*>) {
                                    YTDBComparableSet(propValue.toHashSet())
                                } else {
                                    propValue as Comparable<*>
                                }
                                oEntity.setProperty(propName, comparableValue)
                                properties++
                            }
                        }

                        copyBlobsDuration += measureTime {
                            for (blobName in xEntity.blobNames) {
                                xEntity.getBlob(blobName)?.let { blobValue ->
                                    oEntity.setBlob(blobName, blobValue)
                                    blobs++
                                }
                            }
                        }

                        xEntityIdToOEntityId[xEntity.id] = oEntity.id
                        commitEntitiesDuration += measureTime {
                            countingTx.increment()
                        }

                        if (System.currentTimeMillis() - lastProgressPrintedAt > printProgressAtLeastOnceIn) {
                            log.info { "${typeIdx}/${entityTypes.size} $type current entity $xEntityIdx/$xEntitiesCount, properties copied: $properties, blobs copied: $blobs" }
                            lastProgressPrintedAt = System.currentTimeMillis()
                        }

                        edgeClassesToCreate.addAll(xEntity.linkNames)
                    }

                    // plan to create a sequence to generate localEntityIds for the class
                    sequencesToCreate.add(Pair(localEntityIdSequenceName(type), largestEntityId))

                    log.info { "${typeIdx}/${entityTypes.size} $type entities copied: $xEntitiesCount, properties copied: $properties, blobs copied: $blobs" }
                    totalEntities += xEntitiesCount
                    totalProperties += properties
                    totalBlobs += blobs
                }
                countingTx.commit()
                copyEntitiesPropertiesAndBlobsTransactions = countingTx.transactionsCommited
                log.info { "Entities have been copied. Entity types: ${entityTypesCount}, entities copied: $totalEntities, properties copied: $totalProperties, blobs copied: $totalBlobs" }
            }
        }
        orientProvider.withSession { session ->
            sequencesToCreate.forEach { (name, largestExistingId) ->
                schemaBuddy.getOrCreateSequence(session, name, largestExistingId)
            }
        }
        return edgeClassesToCreate
    }

    private fun createEdgeClassesIfAbsent(edgeClassesToCreate: Set<String>) {
        log.info { "3. Create edge classes" }
        log.info { "${edgeClassesToCreate.size} edge classes to create" }
        orientProvider.withSession { oSession ->
            edgeClassesToCreate.forEachIndexed { i, edgeClassName ->
                log.info { "$i $edgeClassName ${edgeClassName.asEdgeClass} is being copied" }
                oSession.getClass(edgeClassName.asEdgeClass)
                    ?: oSession.createEdgeClass(edgeClassName.asEdgeClass)
            }
        }
        log.info { "${edgeClassesToCreate.size} edge classes have been created" }
    }

    private fun copyLinks() {
        log.info { "4. Copy links" }
        xodus.withReadonlyTx { xTx ->
            orient.withCountingTx(entitiesPerTransaction) { countingTx ->
                val entityTypes = xTx.entityTypes.toSet()
                val entityTypesCount = entityTypes.size
                log.info { "$entityTypesCount entity types to copy links" }
                var totalEntities = 0L
                entityTypes.forEachIndexed { typeIdx, type ->
                    var linksProcessed = 0
                    var linksCopied = 0
                    val xEntities = xTx.getAll(type)
                    val xEntitiesCount = xEntities.size()
                    log.info { "$typeIdx/${entityTypesCount} $type $xEntitiesCount entities to copy" }
                    var lastProgressPrintedAt = System.currentTimeMillis()
                    xEntities.forEachIndexed { xEntityIdx, xEntity ->
                        val oEntityId = xEntityIdToOEntityId.getValue(xEntity.id)
                        val oEntity = orient.getEntity(oEntityId)

                        copyLinksDuration += measureTime {
                            for (linkName in xEntity.linkNames) {
                                val copiedLinks = HashSet<EntityId>()
                                for (xTargetEntity in xEntity.getLinks(linkName)) {
                                    linksProcessed++
                                    if (copiedLinks.contains(xTargetEntity.id)) continue

                                    try {
                                        xTx.getEntity(xTargetEntity.id)
                                        val oTargetId =
                                            xEntityIdToOEntityId.getValue(xTargetEntity.id)
                                        oEntity.addLink(linkName, oTargetId)
                                        copiedLinks.add(xTargetEntity.id)
                                        linksCopied++
                                    } catch (e: EntityRemovedInDatabaseException) {
                                        // ignore
                                    }
                                    if (System.currentTimeMillis() - lastProgressPrintedAt > printProgressAtLeastOnceIn) {
                                        log.info { "${typeIdx}/${entityTypes.size} $type current entity $xEntityIdx/$xEntitiesCount, links processed: $linksProcessed, links copied: $linksCopied" }
                                        lastProgressPrintedAt = System.currentTimeMillis()
                                    }
                                }
                                if (copiedLinks.isNotEmpty()) {
                                    val commitDuration = measureTime {
                                        countingTx.increment()
                                    }
                                    commitLinksDuration += commitDuration
                                    // we want to exclude the commit duration from the copy duration
                                    copyLinksDuration -= commitDuration
                                }
                            }
                        }
                    }
                    log.info { "${typeIdx}/${entityTypes.size} $type entities processed $xEntitiesCount, links processed: $linksProcessed, links copied: $linksCopied" }
                    totalEntities += xEntitiesCount
                    totalLinksProcessed += linksProcessed
                    totalLinksCopied += linksCopied
                }
                countingTx.commit()
                copyLinksTransactions = countingTx.transactionsCommited
                log.info { "Links have been copied. Entity types: ${entityTypes.size}, entities processed: $totalEntities, links processed: $totalLinksProcessed, links copied: $totalLinksCopied" }
            }
        }
    }
}
