package jetbrains.exodus.query.metadata

import jetbrains.exodus.bindings.ComparableSet
import jetbrains.exodus.entitystore.PersistentEntityStore
import jetbrains.exodus.entitystore.orientdb.OPersistentEntityStore
import jetbrains.exodus.entitystore.orientdb.OVertexEntity
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.BINARY_BLOB_CLASS_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.STRING_BLOB_CLASS_NAME
import mu.KotlinLogging
import java.lang.IllegalStateException

private val log = KotlinLogging.logger { }

fun checkDataIsSame(xStore: PersistentEntityStore, oStore: OPersistentEntityStore) {
    DataAfterMigrationChecker(xStore, oStore).checkDataIsSame()
}

internal class DataAfterMigrationChecker(
    private val xStore: PersistentEntityStore,
    private val oStore: OPersistentEntityStore,
) {
    fun checkDataIsSame() {
        checkEntityTypes()
        checkPropertiesAndBlobs()
//        createEdgeClassesIfAbsent(edgeClassesToCreate)
//        copyLinks()
    }

    private fun checkEntityTypes() {
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

    private fun checkPropertiesAndBlobs(): Set<String> {
        val edgeClassesToCreate = HashSet<String>()
        xStore.withReadonlyTx { xTx ->
            for (type in xTx.entityTypes) {
                oStore.withReadonlyTx { oTx ->
                    val xSize = xTx.getAll(type).size()
                    val oSize = oTx.getAll(type).size()
                    require(xSize == oSize) { "different number of $type. xStore - $xSize, oStore - $oSize" }
                    for (e1 in xTx.getAll(type)) {
                        val e2 = oTx.getEntity(e1.id)
                        for (propName in e1.propertyNames) {
                            val v1 = e1.getProperty(propName)
                            val v2 = e2.getProperty(propName)

                            if (v1 is ComparableSet<*>) continue

                            require((v1 == null && v2 == null) || v1?.compareTo(v2) == 0) { "zb" }
                        }
                        for (blobName in e1.blobNames) {
                            e1.getBlob(blobName)?.let { blobValue ->
                            }
                        }
                        edgeClassesToCreate.addAll(e1.linkNames)
                    }
                }
            }
        }
        return edgeClassesToCreate
    }

//    private fun copyLinks() {
//        store1.withReadonlyTx { xTx ->
//            store2.withCountingTx(entitiesPerTransaction) { countingTx ->
//                for (type in xTx.entityTypes) {
//                    for (xEntity in xTx.getAll(type)) {
//                        val oEntityId = xEntityIdToOEntityId.getValue(xEntity.id)
//                        val oEntity = store2.getEntity(oEntityId)
//
//                        var copiedSomeLinks = false
//                        for (linkName in xEntity.linkNames) {
//                            for (xTargetEntity in xEntity.getLinks(linkName)) {
//                                val oTargetId = xEntityIdToOEntityId.getValue(xTargetEntity.id)
//                                oEntity.addLink(linkName, oTargetId)
//                                copiedSomeLinks = true
//                            }
//                        }
//                        if (copiedSomeLinks) {
//                            countingTx.increment()
//                        }
//                    }
//                }
//            }
//        }
//    }
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
