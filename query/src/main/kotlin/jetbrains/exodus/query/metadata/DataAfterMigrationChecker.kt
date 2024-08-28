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

import jetbrains.exodus.bindings.ComparableSet
import jetbrains.exodus.entitystore.EntityId
import jetbrains.exodus.entitystore.EntityRemovedInDatabaseException
import jetbrains.exodus.entitystore.PersistentEntityStore
import jetbrains.exodus.entitystore.orientdb.OPersistentEntityStore
import jetbrains.exodus.entitystore.orientdb.OVertexEntity
import mu.KotlinLogging
import java.io.InputStream
import kotlin.time.Duration
import kotlin.time.measureTime
import kotlin.time.measureTimedValue

private val log = KotlinLogging.logger { }

fun checkDataIsSame(xStore: PersistentEntityStore, oStore: OPersistentEntityStore, xEntityIdToOEntityId: Map<EntityId, EntityId>): DataAfterMigrationCheckingStats {
    return DataAfterMigrationChecker(xStore, oStore, xEntityIdToOEntityId).checkDataIsSame()
}

data class DataAfterMigrationCheckingStats(
    val checkEntityTypesDuration: Duration,

    val checkEntitiesDuration: Duration,
    val findEntitiesDuration: Duration,
    val checkPropertiesDuration: Duration,
    val checkBlobsDuration: Duration,
    val checkLinksDuration: Duration
)

@OptIn(ExperimentalStdlibApi::class)
internal class DataAfterMigrationChecker(
    private val xStore: PersistentEntityStore,
    private val oStore: OPersistentEntityStore,
    private val xEntityIdToOEntityId: Map<EntityId, EntityId>,
    private val printProgressAtLeastOnceIn: Int = 5_000
) {
    private var findEntitiesDuration = Duration.ZERO
    private var checkPropertiesDuration = Duration.ZERO
    private var checkBlobsDuration = Duration.ZERO
    private var checkLinksDuration = Duration.ZERO

    fun checkDataIsSame(): DataAfterMigrationCheckingStats {
        val checkEntityTypesDuration = measureTime {
            checkEntityTypes()
        }
        val checkEntitiesDuration = measureTime {
            checkPropertiesBlobsAndLinks()
        }
        return DataAfterMigrationCheckingStats(
            checkEntityTypesDuration = checkEntityTypesDuration,

            checkEntitiesDuration = checkEntitiesDuration,
            findEntitiesDuration = findEntitiesDuration,
            checkPropertiesDuration = checkPropertiesDuration,
            checkBlobsDuration = checkBlobsDuration,
            checkLinksDuration = checkLinksDuration
        )
    }

    private fun checkEntityTypes() {
        log.info { "1. Check entity types" }
        xStore.withReadonlyTx { xTx ->
            oStore.withReadonlyTx { oTx ->
                val xTypes = xTx.entityTypes.toSet()
                val oTypes = oTx.entityTypes.toSet()
                    .filterNot { it.contains(OVertexEntity.EDGE_CLASS_SUFFIX) } - oEntityStoreExtraEntityTypes
                log.info { "xStore entity types: ${xTypes.size}" }
                log.info { "oStore entity types excluding extra types: ${xTypes.size}" }
                when {
                    xTypes.size > oTypes.size || !oTypes.containsAll(xTypes) -> throw IllegalStateException("oStore is missing the following entity types from xStore: ${xTypes - oTypes}")
                    oTypes.size > xTypes.size -> throw IllegalStateException("xStore is missing the following entity types from oStore: ${oTypes - xTypes}")
                    else -> Unit
                }
            }
        }
    }

    @OptIn(ExperimentalStdlibApi::class)
    private fun checkPropertiesBlobsAndLinks() {
        log.info { "2. Check properties, blobs and links" }
        xStore.withReadonlyTx { xTx ->
            val entityTypes = xTx.entityTypes.toSet()
            log.info { "${entityTypes.size} entity types to process" }
            entityTypes.forEachIndexed { typeIdx, type ->
                val xEntities = xTx.getAll(type)

                val xSize = xEntities.size()
                log.info { "${typeIdx}/${entityTypes.size} $type $xSize entities to process" }
                val oSize = oStore.withReadonlyTx { oTx -> oTx.getAll(type).size() }
                require(xSize == oSize) { "different number of $type entities. xStore - $xSize, oStore - $oSize" }

                var lastProgressPrintedAt = System.currentTimeMillis()
                xEntities.forEachIndexed { entityIdx, e1 ->
                    // oStore does not close resultSets, it causes a flood in the log and out of memory crash,
                    // so we have to process entity by entity until it is fixed :(
                    oStore.withReadonlyTx { oTx ->
                        val (e2, findEntityDuration) = measureTimedValue {
                            oTx.getEntity(xEntityIdToOEntityId.getValue(e1.id))
                        }
                        findEntitiesDuration += findEntityDuration

                        checkPropertiesDuration += measureTime {
                            for (propName in e1.propertyNames) {
                                val v1 = e1.getProperty(propName)
                                val v2 = e2.getProperty(propName)

                                when {
                                    v1 is ComparableSet<*> -> {
                                        checkSets(
                                            setsInfo = "$type $entityIdx/$xSize ${e1.id} $propName sets",
                                            xSet = v1.toSet(),
                                            oSet = (v2 as ComparableSet<*>).toSet()
                                        )
                                    }
                                    v1 is String -> {
                                        checkStrings(
                                            strsInfo = "$type $entityIdx/$xSize ${e1.id} $propName strings",
                                            xStr = v1,
                                            oStr = v2 as String,
                                            xGetRawBytes = { e1.getRawProperty(propName)?.bytesUnsafe!! }
                                        )
                                    }
                                    else -> {
                                        require((v1 == null && v2 == null) || v1?.compareTo(v2) == 0) {
                                            """
                                                $type $entityIdx/$xSize ${e1.id} $propName properties are different. 
                                                xStore type: ${v1?.javaClass}, oStore type: ${v2?.javaClass}
                                                xStore value: '${v1}', oStore value: '${v2}'
                                                comparison result ${v1?.compareTo(v2)}
                                            """.trimIndent()
                                        }
                                    }
                                }
                            }
                        }

                        checkBlobsDuration += measureTime {
                            for (blobName in e1.blobNames) {
                                checkBlobs(
                                    blobsInfo = "$type $entityIdx/$xSize ${e1.id} $blobName blobs",
                                    xBlob = e1.getBlob(blobName),
                                    oBlob = e2.getBlob(blobName)
                                )
                            }
                        }

                        checkLinksDuration += measureTime {
                            for (linkName in e1.linkNames) {
                                val xTargetEntities = e1.getLinks(linkName).filter { xTargetEntity ->
                                    // filter "dead" links
                                    try {
                                        xTx.getEntity(xTargetEntity.id)
                                        true
                                    } catch (e: EntityRemovedInDatabaseException) {
                                        false
                                    }
                                }.map { it.id.toString() }.toSet()
                                val oTargetEntities = e2.getLinks(linkName).map { it.id.toString() }
                                require(
                                    xTargetEntities.size == oTargetEntities.size
                                            && xTargetEntities.containsAll(oTargetEntities)
                                            && oTargetEntities.containsAll(xTargetEntities)
                                ) {
                                    "$type $entityIdx/$xSize ${e1.id} $linkName links are different. xStore: ${xTargetEntities.size}, oStore: ${oTargetEntities.size}"
                                }
                            }
                        }
                    }
                    if (System.currentTimeMillis() - lastProgressPrintedAt > printProgressAtLeastOnceIn) {
                        log.info { "${typeIdx}/${entityTypes.size} $type $entityIdx/$xSize has been processed" }
                        lastProgressPrintedAt = System.currentTimeMillis()
                    }
                }
            }
        }
    }

    private fun checkStrings(strsInfo: String, xStr: String, oStr: String, xGetRawBytes: () -> ByteArray) {
        if (xStr == oStr) {
            return
        }

        // if they are not equal, it may be because of the broken Classic Xodus string
        // let's try to fix it
        val xFixedStr = xStr.encodeToByteArray(throwOnInvalidSequence = false).decodeToString(throwOnInvalidSequence = true)

        require(xFixedStr == oStr) {
            val rawBytes = xGetRawBytes().toHexString()
            val xHex = xStr.encodeToByteArray(throwOnInvalidSequence = false).toHexString()
            val xFixedHex = xFixedStr.encodeToByteArray(throwOnInvalidSequence = false).toHexString()
            val oHex = oStr.encodeToByteArray(throwOnInvalidSequence = false).toHexString()
            """
                $strsInfo are different. 
                xStore: '${xStr}'
                xStore fixed: '${xFixedStr}'
                oStore: '${oStr}'
                xStore bytes: '${xHex}'
                xStore fixed bytes: '${xFixedHex}'
                oStore bytes: '${oHex}'
                xStore raw bytes: '${rawBytes}'
            """.trimIndent()
        }
    }

    private fun checkSets(setsInfo: String, xSet: Set<*>, oSet: Set<*>) {
        if (xSet.containsAll(oSet) && oSet.containsAll(xSet)) {
            return
        }

        // xSet may contain some broken Classic Xodus strings
        // let's fix them and try again
        val fixedXSet = xSet.map {
            if (it is String) {
                it.encodeToByteArray(throwOnInvalidSequence = false).decodeToString(throwOnInvalidSequence = true)
            } else {
                it
            }
        }.toSet()

        val xSetRemainder = fixedXSet - oSet
        val oSetRemainder = oSet - fixedXSet

        check(xSetRemainder.isEmpty() && oSetRemainder.isEmpty()) {
            """
                $setsInfo are different
                xStore: ${xSet.map { it.toString() }.sorted().joinToString(", ")}
                oStore: ${oSet.map { it.toString() }.sorted().joinToString(", ")}
                xStore - oStore: ${xSetRemainder.map { it.toString() }.sorted().joinToString(", ")}
                oStore - xStore: ${oSetRemainder.map { it.toString() }.sorted().joinToString(", ")}
            """.trimIndent()
        }
    }

    private fun checkBlobs(blobsInfo: String, xBlob: InputStream?, oBlob: InputStream?) {
        if (xBlob == null && oBlob == null) return

        val xBytes = xBlob?.readAllBytes()
        val oBytes = oBlob?.readAllBytes()
        check(xBytes.contentEquals(oBytes)) {
            val xHex = xBytes?.toHexString()
            val oHex = oBytes?.toHexString()
            """
                $blobsInfo are different
                xStore: $xHex
                oStore: $oHex
            """.trimIndent()
        }
    }
}

private val oEntityStoreExtraEntityTypes = setOf(
    "ORole",
    "E",
    "OIdentity",
    "OFunction",
    "V",
    "OTriggered",
    "ORestricted",
    "OSchedule",
    "OSequence",
    "OSecurityPolicy",
    "OUser",
)
