package jetbrains.exodus.query.metadata

import jetbrains.exodus.bindings.ComparableSet
import jetbrains.exodus.entitystore.EntityRemovedInDatabaseException
import jetbrains.exodus.entitystore.PersistentEntityStore
import jetbrains.exodus.entitystore.orientdb.OComparableSet
import jetbrains.exodus.entitystore.orientdb.OPersistentEntityStore
import jetbrains.exodus.entitystore.orientdb.OVertexEntity
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.BINARY_BLOB_CLASS_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.STRING_BLOB_CLASS_NAME
import mu.KotlinLogging
import java.io.InputStream
import java.lang.IllegalStateException
import kotlin.collections.HashSet

private val log = KotlinLogging.logger { }

fun checkDataIsSame(xStore: PersistentEntityStore, oStore: OPersistentEntityStore) {
    DataAfterMigrationChecker(xStore, oStore).checkDataIsSame()
}

internal class DataAfterMigrationChecker(
    private val xStore: PersistentEntityStore,
    private val oStore: OPersistentEntityStore,
    private val printProgressAtLeastOnceIn: Int = 5_000
) {
    fun checkDataIsSame() {
        checkEntityTypes()
        checkPropertiesBlobsAndLinks()
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

    private fun checkPropertiesBlobsAndLinks() {
        log.info { "2. Check properties, blobs and links" }
        val edgeClassesToCreate = HashSet<String>()
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
                        val e2 = oTx.getEntity(e1.id)
                        for (propName in e1.propertyNames) {
                            val v1: Comparable<Any?>? = e1.getProperty(propName)
                            val v2: Comparable<Any?>? = e2.getProperty(propName)

                            if (v1 is ComparableSet<*>) {
                                val set1 = v1.toSet()
                                val set2 = (v2 as OComparableSet<*>).toSet()
                                require(set1.containsAll(set2) && set2.containsAll(set1)) {
                                    "$type $entityIdx/$xSize ${e1.id} $propName content is different. " +
                                            "xStore: ${set1.map { it.toString() }.sorted().joinToString(", ")}. " +
                                            "oStore: ${set2.map { it.toString() }.sorted().joinToString(", ")}"
                                }
                            } else {
                                require((v1 == null && v2 == null) || v1?.compareTo(v2) == 0) { "$type $entityIdx/$xSize ${e1.id} $propName is different. xStore: $v1. oStore: $v2" }
                            }
                        }
                        for (blobName in e1.blobNames) {
                            val blob1: InputStream? = e1.getBlob(blobName)
                            val blob2: InputStream? = e2.getBlob(blobName)
                            when {
                                blob1 == null && blob2 == null -> Unit
                                blob1 != null && blob2 != null && blobsEqual(blob1, blob2) -> Unit
                                else -> throw IllegalStateException("$type $entityIdx/$xSize ${e1.id} $blobName are different")
                            }
                        }
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
                            require(xTargetEntities.size == oTargetEntities.size && xTargetEntities.containsAll(oTargetEntities) && oTargetEntities.containsAll(xTargetEntities)) {
                                "$type $entityIdx/$xSize ${e1.id} $linkName links are different. xStore: ${xTargetEntities.size}, oStore: ${oTargetEntities.size}"
                            }
                        }
                        edgeClassesToCreate.addAll(e1.linkNames)
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
    BINARY_BLOB_CLASS_NAME,
    STRING_BLOB_CLASS_NAME
)
