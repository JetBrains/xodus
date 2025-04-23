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
package jetbrains.exodus.entitystore.youtrackdb

import com.jetbrains.youtrack.db.api.record.Direction
import com.jetbrains.youtrack.db.api.record.Edge
import com.jetbrains.youtrack.db.api.record.Identifiable
import com.jetbrains.youtrack.db.api.record.RID
import com.jetbrains.youtrack.db.api.record.Vertex
import com.jetbrains.youtrack.db.api.schema.SchemaClass
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedMultiValue
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.LinkBag
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClassInternal
import com.jetbrains.youtrack.db.internal.core.record.impl.RecordBytes
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.EntityId
import jetbrains.exodus.entitystore.EntityIterable
import jetbrains.exodus.entitystore.EntityRemovedInDatabaseException
import jetbrains.exodus.entitystore.youtrackdb.YTDBVertexEntity.Companion.CLASS_ID_CUSTOM_PROPERTY_NAME
import jetbrains.exodus.entitystore.youtrackdb.YTDBVertexEntity.Companion.LOCAL_ENTITY_ID_PROPERTY_NAME
import jetbrains.exodus.entitystore.youtrackdb.YTDBVertexEntity.Companion.linkTargetEntityIdPropertyName
import jetbrains.exodus.entitystore.youtrackdb.iterate.link.YTDBLinksFromEntityIterable
import jetbrains.exodus.entitystore.youtrackdb.iterate.link.YTDBVertexEntityIterable
import jetbrains.exodus.util.LightByteArrayOutputStream
import jetbrains.exodus.util.UTFUtil
import mu.KLogging
import java.io.ByteArrayInputStream
import java.io.File
import java.io.InputStream
import kotlin.jvm.optionals.getOrNull

open class YTDBVertexEntity(vertex: Vertex, private val store: YTDBEntityStore) : YTDBEntity {

    companion object : KLogging() {
        const val EDGE_CLASS_SUFFIX = "_link"
        private const val LINK_TARGET_ENTITY_ID_PROPERTY_NAME_SUFFIX = "_targetEntityId"
        private const val BLOB_SIZE_PROPERTY_NAME_SUFFIX = "_blob_size"
        private const val STRING_BLOB_HASH_PROPERTY_NAME_SUFFIX = "_string_blob_hash"
        fun blobSizeProperty(propertyName: String) =
            "_$propertyName$BLOB_SIZE_PROPERTY_NAME_SUFFIX"

        fun blobHashProperty(propertyName: String) =
            "_$propertyName$STRING_BLOB_HASH_PROPERTY_NAME_SUFFIX"

        // Backward compatible EntityId

        const val CLASS_ID_CUSTOM_PROPERTY_NAME = "classId"
        const val CLASS_ID_SEQUENCE_NAME = "sequence_classId"

        const val LOCAL_ENTITY_ID_PROPERTY_NAME = "localEntityId"
        val IGNORED_PROPERTY_NAMES = setOf(LOCAL_ENTITY_ID_PROPERTY_NAME)
        val IGNORED_SUFFIXES = setOf(
            LINK_TARGET_ENTITY_ID_PROPERTY_NAME_SUFFIX,
            BLOB_SIZE_PROPERTY_NAME_SUFFIX,
            STRING_BLOB_HASH_PROPERTY_NAME_SUFFIX
        )

        fun localEntityIdSequenceName(className: String): String =
            "${className}_sequence_localEntityId"

        fun edgeClassName(className: String): String {
            // YouTrack has fancy link names like '__CUSTOM_FIELD__Country/Region_227'. OrientDB does not like symbols
            // like '/' in class names. So we have to get rid of them.
            val sanitizedClassName = className.replace('/', '_')
            return "$sanitizedClassName$EDGE_CLASS_SUFFIX"
        }

        fun linkTargetEntityIdPropertyName(linkName: String): String {
            return "$linkName$LINK_TARGET_ENTITY_ID_PROPERTY_NAME_SUFFIX"
        }
    }

    private var vertexRecord: Vertex
    private var oEntityId: RIDEntityId

    init {
        val v = if (vertex.isUnloaded) {
            val session = store.requireActiveTransaction()
            session.bindToSession(vertex)
        } else {
            vertex
        }

        oEntityId = RIDEntityId.fromVertex(v)
        vertexRecord = v
    }

    val vertex: Vertex
        get() {
            if (vertexRecord.isUnloaded) {
                val session = store.requireActiveTransaction()
                vertexRecord = session.bindToSession(vertexRecord)
            }

            return vertexRecord
        }

    val isUnloaded: Boolean
        get() = vertexRecord.isUnloaded


    override fun getStore() = store

    override fun getId(): YTDBEntityId = oEntityId

    override fun toIdString(): String = oEntityId.toString()

    override fun getType(): String = oEntityId.getTypeName()

    override fun delete(): Boolean {
        requireActiveWritableTransaction()
        vertex.delete()
        return true
    }

    override fun resetToNew() {
        val className = vertexRecord.schemaClassName
        vertexRecord = (store.databaseSession as DatabaseSessionInternal).newVertex(className)
    }

    override fun generateId() {
        val type = oEntityId.getTypeName()
        store.requireActiveTransaction().generateEntityId(type, vertexRecord)
        oEntityId = RIDEntityId.fromVertex(vertexRecord)
    }

    private fun requireActiveTx(): YTDBStoreTransaction {
        return store.requireActiveTransaction()
    }

    override fun getRawProperty(propertyName: String): ByteIterable? {
        requireActiveTx()
        TODO()
    }

    override fun getProperty(propertyName: String): Comparable<*>? {
        requireActiveTx()
        val value = vertex.getProperty<Any>(propertyName)
        return if (value == null || value !is MutableSet<*>) {
            value as Comparable<*>?
        } else {
            YTDBComparableSet(value)
        }
    }

    override fun setProperty(propertyName: String, value: Comparable<*>): Boolean {
        requireActiveWritableTransaction()
        val oldValue = vertex.getProperty<Any>(propertyName)

        if (value is MutableSet<*> || oldValue is MutableSet<*>) {
            return setPropertyAsSet(propertyName, value)
        } else if (oldValue == value) {
            return false
        } else {
            vertex.setProperty(propertyName, value)
            return true
        }
    }

    private fun setPropertyAsSet(propertyName: String, newValue: Any?): Boolean {
        val set = when (newValue) {
            is YTDBComparableSet<*> -> newValue.source
            is MutableSet<*> -> newValue
            else -> throw IllegalArgumentException("Unexpected value: $newValue")
        }
        val databaseSet =
            if (set is TrackedMultiValue<*, *>) set
            else {
                val it = set.iterator()
                if (it.hasNext() && it.next() is Identifiable) {
                    // we can suppress unchecked cast here because "newLinkSet" validates the input
                    @Suppress("UNCHECKED_CAST")
                    store.databaseSession.newLinkSet(set as MutableSet<Identifiable>)
                } else {
                    store.databaseSession.newEmbeddedSet(set)
                }
            }

        vertex.setProperty(propertyName, databaseSet)

        return vertex.getProperty<TrackedMultiValue<*, *>>(propertyName)?.isTransactionModified == true
    }

    override fun deleteProperty(propertyName: String): Boolean {
        requireActiveWritableTransaction()
        if (vertex.hasProperty(propertyName)) {
            vertex.removeProperty<Any>(propertyName)
            return true
        } else {
            return false
        }
    }

    override fun getPropertyNames(): List<String> {
        requireActiveTx()
        val allPropertiesNames = vertex.propertyNames
        return allPropertiesNames
            .filter { propName ->
                //not ignored properties
                !IGNORED_PROPERTY_NAMES.contains(propName)
                        && !allPropertiesNames.contains(blobHashProperty(propName))
                        && !allPropertiesNames.contains(blobSizeProperty(propName))
                        && !IGNORED_SUFFIXES.any { suffix -> propName.endsWith(suffix) }
            }
            .toList()
    }

    override fun getBlob(blobName: String): InputStream? {
        requireActiveTx()
        val blob: RecordBytes = vertex.getProperty(blobName) ?: return null
        return ByteArrayInputStream(blob.toStream())
    }

    override fun getBlobSize(blobName: String): Long {
        requireActiveTx()

        return vertex.getProperty(blobSizeProperty(blobName)) ?: -1
    }

    override fun setBlob(blobName: String, blob: InputStream) {
        requireActiveWritableTransaction()

        if (vertex.hasProperty(blobName)) {
            vertex.removeProperty<Any>(blobName)
        }

        val allBytes = blob.readAllBytes()
        val oBlob = store.databaseSession.activeTransaction.newBlob(allBytes)
        vertex.setProperty(blobName, oBlob)
        vertex.setProperty(blobSizeProperty(blobName), allBytes.size.toLong())
    }

    override fun deleteBlob(blobName: String): Boolean {
        requireActiveWritableTransaction()
        if (vertex.hasProperty(blobName)) {
            vertex.removeProperty<Any>(blobName)
            vertex.removeProperty<Any>(blobSizeProperty(blobName))
            vertex.removeProperty<Any>(blobHashProperty(blobName))
            return true
        }
        return false
    }

    override fun getBlobString(blobName: String): String? {
        requireActiveTx()
        val blob: RecordBytes = vertex.getProperty(blobName) ?: return null
        return UTFUtil.readUTF(ByteArrayInputStream(blob.toStream()))
    }

    override fun setBlob(blobName: String, file: File) {
        setBlob(blobName, file.inputStream())
    }

    /**
     * Stores the string in the modified UTF-8 format
     */
    override fun setBlobString(blobName: String, blobString: String): Boolean {
        requireActiveWritableTransaction()

        // toByteArray() will not copy data
        val baos = LightByteArrayOutputStream(blobString.length)
        UTFUtil.writeUTF(baos, blobString)

        // we know the exact size only when we encoded the string to UTF.
        // so, here we can check if we already have the same one
        if (vertex.hasProperty(blobName)) {
            val oldHash = vertex.getProperty<Int>(blobHashProperty(blobName))
            val oldLen = vertex.getProperty<Long>(blobSizeProperty(blobName))
            if (oldHash == blobString.hashCode() && oldLen == baos.size().toLong()) {
                return false
            }
            vertex.removeProperty<Any>(blobName)
        }

        val oBlob = store.databaseSession.activeTransaction.newBlob(baos.toByteArray())
        vertex.setProperty(blobName, oBlob)
        vertex.setProperty(blobHashProperty(blobName), blobString.hashCode())
        vertex.setProperty(blobSizeProperty(blobName), baos.size().toLong())
        return true
    }

    override fun getBlobNames(): List<String> {
        requireActiveTx()
        return vertex.propertyNames
            .filter { it.endsWith(BLOB_SIZE_PROPERTY_NAME_SUFFIX) }
            .map { it.substring(1).substringBefore(BLOB_SIZE_PROPERTY_NAME_SUFFIX) }
    }

    // Add links

    override fun addLink(linkName: String, target: Entity): Boolean {
        val currentTx = requireActiveWritableTransaction()
        require(target is YTDBVertexEntity) { "Only OVertexEntity is supported, but was ${target.javaClass.simpleName}" }
        return currentTx.addLinkImpl(linkName, target.vertex)
    }

    override fun addLink(linkName: String, targetId: EntityId): Boolean {
        val currentTx = requireActiveWritableTransaction()
        val targetOId = store.requireOEntityId(targetId)
        if (targetOId == RIDEntityId.EMPTY_ID) {
            return false
        }
        try {
            val target = currentTx.getRecord<Vertex>(targetOId)
            return currentTx.addLinkImpl(linkName, target)
        } catch (e: EntityRemovedInDatabaseException) {
            return false
        }
    }

    private fun YTDBStoreTransaction.addLinkImpl(linkName: String, target: Vertex): Boolean {
        val outClassName = vertex.requireSchemaClass().name
        val inClassName = target.requireSchemaClass().name
        val edgeClass = getOrCreateEdgeClass(linkName, outClassName, inClassName)
        val edgeClassName = edgeClassName(linkName)

        /*
        We check for duplicates only if there is an appropriate index for it.
        Without an index, performance degradation will be catastrophic.

        You may ask why not to throw an exception if there is no an index?
        Well, we have the data migration process. During this process:
        1. We do not have any indices
        2. Skipping this findEdge(...) call is exactly what we want from the performance point of view.
        3. We avoid duplicates explicitly.
        Well, during the data migration process, there are no any indices and
        skipping this findEdge(...) call is exactly what we need.
         */
        val currentEdge: Edge? =
            if ((edgeClass as SchemaClassInternal).areIndexed(
                    databaseSession as DatabaseSessionInternal,
                    Edge.DIRECTION_IN,
                    Edge.DIRECTION_OUT
                )
            ) {
                findEdge(edgeClassName, target.identity)
            } else null

        if (currentEdge == null) {
            vertex.addEdge(target, edgeClassName)
            // If the link is indexed, we have to update the complementary internal property.
            vertex.addTargetEntityIdIfLinkIndexed(linkName, target.identity)
            return true
        } else {
            return false
        }
    }

    private fun Vertex.addTargetEntityIdIfLinkIndexed(linkName: String, targetId: RID) {
        val linkTargetEntityIdPropertyName = linkTargetEntityIdPropertyName(linkName)
        if (requireSchemaClass().existsProperty(linkTargetEntityIdPropertyName)) {
            val bag = getProperty<LinkBag>(linkTargetEntityIdPropertyName)
                ?: LinkBag(boundedToSession as DatabaseSessionInternal)
            bag.add(targetId)
            setProperty(linkTargetEntityIdPropertyName, bag)
        }
    }


    // Delete links

    override fun deleteLink(linkName: String, target: Entity): Boolean {
        val currentTx = requireActiveWritableTransaction()
        target as YTDBVertexEntity
        val targetOId = target.oEntityId.asOId()
        return currentTx.deleteLinkImpl(linkName, targetOId)
    }

    override fun deleteLink(linkName: String, targetId: EntityId): Boolean {
        val currentTx = requireActiveWritableTransaction()
        val targetOId = store.requireOEntityId(targetId).asOId()
        return currentTx.deleteLinkImpl(linkName, targetOId)
    }

    override fun deleteLinks(linkName: String) {
        requireActiveWritableTransaction()
        val edgeClassName = edgeClassName(linkName)
        vertex.getEdges(Direction.OUT, edgeClassName).forEach {
            it.delete()
        }
        vertex.deleteAllTargetEntityIdsIfLinkIndexed(linkName)
    }

    private fun YTDBStoreTransaction.deleteLinkImpl(linkName: String, targetId: RID): Boolean {
        val edgeClassName = edgeClassName(linkName)

        val edge = findEdge(edgeClassName, targetId)
        if (edge != null) {
            edge.delete()
            // if the link in a composite index, we have to update the complementary internal property.
            vertex.deleteTargetEntityIdIfLinkIndexed(linkName, targetId)
            return true
        }

        return false
    }

    private fun Vertex.deleteTargetEntityIdIfLinkIndexed(linkName: String, targetId: RID) {
        val linkTargetEntityIdPropertyName = linkTargetEntityIdPropertyName(linkName)
        if (requireSchemaClass().existsProperty(linkTargetEntityIdPropertyName)) {
            val bag = getProperty<LinkBag>(linkTargetEntityIdPropertyName)
                ?: LinkBag(boundedToSession as DatabaseSessionInternal)
            bag.remove(targetId)
            setProperty(linkTargetEntityIdPropertyName, bag)
        }
    }

    private fun Vertex.deleteAllTargetEntityIdsIfLinkIndexed(linkName: String) {
        val propName = linkTargetEntityIdPropertyName(linkName)
        if (requireSchemaClass().existsProperty(propName)) {
            setProperty(propName, LinkBag(boundedToSession as DatabaseSessionInternal))
        }
    }


    // Set links

    override fun setLink(linkName: String, target: Entity?): Boolean {
        val currentTx = requireActiveWritableTransaction()
        require(target is YTDBVertexEntity?) { "Only OVertexEntity is supported, but was ${target?.javaClass?.simpleName}" }
        return currentTx.setLinkImpl(linkName, target?.vertex)
    }

    override fun setLink(linkName: String, targetId: EntityId): Boolean {
        val currentTx = requireActiveWritableTransaction()
        val targetOId = store.requireOEntityId(targetId)
        if (targetOId == RIDEntityId.EMPTY_ID) {
            return false
        }
        try {
            val target = currentTx.getRecord<Vertex>(targetOId)
            return currentTx.setLinkImpl(linkName, target)
        } catch (e: EntityRemovedInDatabaseException) {
            return false
        }
    }

    private fun YTDBStoreTransaction.setLinkImpl(linkName: String, target: Vertex?): Boolean {
        val currentLink = getLinkImpl(linkName)

        if (currentLink == target) {
            return false
        }
        if (currentLink != null) {
            deleteLinkImpl(linkName, currentLink.identity)
        }
        if (target != null) {
            addLinkImpl(linkName, target)
        }
        return true
    }

    // Get links

    override fun getLink(linkName: String): Entity? {
        requireActiveTx()
        return getLinkImpl(linkName).toOEntityOrNull()
    }

    private fun getLinkImpl(linkName: String): Vertex? {
        val edgeClassName = edgeClassName(linkName)
        return vertex.getVertices(Direction.OUT, edgeClassName).firstOrNull()
    }

    override fun getLinks(linkName: String): EntityIterable {
        val txn = requireActiveTx()
        val edgeClassName = edgeClassName(linkName)
        val links = vertex.getVertices(Direction.OUT, edgeClassName)
        return YTDBVertexEntityIterable(txn, links, store, linkName, this.oEntityId)
    }

    //todo this method should return iterable of different type
    override fun getLinks(linkNames: Collection<String>): EntityIterable {
        requireActiveTx()
        val tx = requireActiveTx()
        return if (linkNames.size == 1) {
            getLinks(linkNames.first())
        } else {
            linkNames.drop(1)
                .fold(
                    YTDBLinksFromEntityIterable(
                        tx,
                        linkNames.first(),
                        this.oEntityId
                    ) as EntityIterable
                ) { res, edgeName ->
                    res.union(YTDBLinksFromEntityIterable(tx, edgeName, this.oEntityId))
                }
        }
    }

    override fun getLinkNames(): List<String> {
        requireActiveTx()
        return ArrayList(
            vertex.getEdgeNames(Direction.OUT)
                .filter { it.endsWith(EDGE_CLASS_SUFFIX) }
                .map { it.substringBefore(EDGE_CLASS_SUFFIX) })
    }

    private fun YTDBStoreTransaction.findEdge(edgeClassName: String, targetId: RID): Edge? {
        val query =
            "SELECT FROM $edgeClassName WHERE outV() = :outId AND inV() = :inId"
        val result = query(query, mapOf("outId" to vertex.identity, "inId" to targetId))
        val foundEdge = result.edgeStream().findFirst()
        return foundEdge.getOrNull()
    }

    override fun compareTo(other: Entity) = id.compareTo(other.id)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is YTDBEntity) return false
        if (javaClass != other.javaClass) return false

        return this.id == other.id
    }

    override fun hashCode(): Int {
        return id.hashCode()
    }

    override val isLoaded = !vertex.isUnloaded

    protected open fun requireActiveWritableTransaction(): YTDBStoreTransaction {
        return store.requireActiveWritableTransaction()
    }

    private fun Vertex?.toOEntityOrNull(): YTDBEntity? = this?.let { YTDBVertexEntity(this, store) }
}

fun SchemaClass.requireClassId(): Int {
    return getCustom(CLASS_ID_CUSTOM_PROPERTY_NAME)?.toInt()
        ?: throw IllegalStateException("classId not found for ${this.name}")
}

fun Vertex.getTargetLocalEntityIds(linkName: String): LinkBag {
    return getProperty<LinkBag>(linkTargetEntityIdPropertyName(linkName))
        ?: LinkBag(boundedToSession as DatabaseSessionInternal)
}

fun Vertex.setTargetLocalEntityIds(linkName: String, ids: LinkBag) {
    setProperty(linkTargetEntityIdPropertyName(linkName), ids)
}

fun Vertex.requireSchemaClass(): SchemaClass {
    return schemaClass ?: throw IllegalStateException("schemaClass not found for $this")
}

fun Vertex.requireLocalEntityId(): Long {
    return getProperty<Long>(LOCAL_ENTITY_ID_PROPERTY_NAME)
        ?: throw IllegalStateException("localEntityId not found for the vertex")
}


val String.asEdgeClass get() = YTDBVertexEntity.edgeClassName(this)
