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
import jetbrains.exodus.entitystore.orientdb.OComparableSet
import jetbrains.exodus.entitystore.orientdb.OPersistentEntityStore
import jetbrains.exodus.entitystore.orientdb.OVertexEntity
import mu.KotlinLogging
import java.io.InputStream
import java.nio.charset.Charset
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
                                        val set1 = v1.toSet()
                                        val set2 = (v2 as OComparableSet<*>).toSet()
                                        require(set1.containsAll(set2) && set2.containsAll(set1)) {
                                            """
                                                $type $entityIdx/$xSize ${e1.id} $propName content is different.
                                                xStore: ${set1.map { it.toString() }.sorted().joinToString(", ")}.
                                                oStore: ${set2.map { it.toString() }.sorted().joinToString(", ")}
                                            """.trimIndent()
                                        }
                                    }
                                    v1 is String -> {
                                        require(v2 is String && v1.compareTo(v2) == 0) {
                                            v2 as String
                                            val charsets = with(Charsets) {
                                                listOf(US_ASCII, ISO_8859_1,
                                                    UTF_16, UTF_32, UTF_8, UTF_16BE, UTF_16LE, UTF_32BE, UTF_32LE)
                                            }
                                            for (charset in charsets) {
                                               try {
                                                   log.info { "decoding with ${charset.name()}" }
                                                   val b1 = try {
                                                       v1.toByteArray(charset)
                                                   } catch (e: Throwable) {
                                                       log.error(e) { "error on encoding v1 to byte array with ${charset.name()}: ${e.message}" }
                                                       null
                                                   }
                                                   val b2 = try {
                                                       v2.toByteArray(charset)
                                                   } catch (e: Throwable) {
                                                       log.error(e) { "error on encoding v2 to byte array with ${charset.name()}: ${e.message}" }
                                                       null
                                                   }
                                                   if (b1 == null || b2 == null) {
                                                       log.info { "one of the strings failed to encode with ${charset.name()}, so go to the next one" }
                                                       continue
                                                   }

                                                   val hex1 = b1.toHexString()
                                                   val hex2 = b2.toHexString()
                                                   if (hex1 != hex2) {
                                                       log.info {
                                                           """
                                                               hex1 != hex2 for ${charset.name()}
                                                               hex1: $hex1
                                                               hex2: $hex2
                                                           """.trimIndent()
                                                       }
                                                   }
                                               } catch (e: Throwable) {
                                                   log.error(e) { "error while decoding with ${charset.name()}: ${e.message}" }
                                               }
                                            }
                                            val b1 = v1.encodeToByteArray(throwOnInvalidSequence = false)
                                            val b2 = v2.encodeToByteArray(throwOnInvalidSequence = false)
                                            val hex1 = b1.toHexString()
                                            val hex2 = b2.toHexString()
                                            check(hex1 == hex2) { "hex is different, hex1: '$hex1', hex2: '$hex2'" }

                                            """
                                                $type $entityIdx/$xSize ${e1.id} $propName is different. 
                                                xStore type: ${v1.javaClass}, oStore type: ${v2.javaClass}
                                                xStore value: '${v1}', oStore value: '${v2}'
                                                xStore hash: ${v1.hashCode()}, oStore hash: ${v2.hashCode()}
                                                ==: ${v1 == v2}
                                                xStore bytes: '${hex1}'
                                                oStore bytes: '${hex2}'
                                                comparison result ${v1.compareTo(v2)}
                                            """.trimIndent()
                                        }

                                    }
                                    else -> {
                                        require((v1 == null && v2 == null) || v1?.compareTo(v2) == 0) {
                                            """
                                                $type $entityIdx/$xSize ${e1.id} $propName is different. 
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
                                val blob1: InputStream? = e1.getBlob(blobName)
                                val blob2: InputStream? = e2.getBlob(blobName)
                                when {
                                    blob1 == null && blob2 == null -> Unit
                                    blob1 != null && blob2 != null && blobsEqual(blob1, blob2) -> Unit
                                    else -> throw IllegalStateException("$type $entityIdx/$xSize ${e1.id} $blobName are different")
                                }
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

    private fun blobsEqual(blob1: InputStream, blob2: InputStream, chunkSize: Int = 1024): Boolean {
        val buffer1 = ByteArray(chunkSize)
        val buffer2 = ByteArray(chunkSize)

        try {
            while (true) {
                val bytesRead1 = blob1.read(buffer1)
                val bytesRead2 = blob2.read(buffer2)

                if (bytesRead1 != bytesRead2) return false
                if (!buffer1.contentEquals(buffer2)) return false
                if (bytesRead1 == -1) break
            }
        } finally {
            blob1.close()
            blob2.close()
        }

        return true
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
