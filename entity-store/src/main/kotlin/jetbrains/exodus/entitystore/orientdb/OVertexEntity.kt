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
package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.db.record.OTrackedSet
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag
import com.orientechnologies.orient.core.id.ORID
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.record.ODirection
import com.orientechnologies.orient.core.record.OEdge
import com.orientechnologies.orient.core.record.OVertex
import com.orientechnologies.orient.core.record.impl.ORecordBytes
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.EntityId
import jetbrains.exodus.entitystore.EntityIterable
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.CLASS_ID_CUSTOM_PROPERTY_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.LOCAL_ENTITY_ID_PROPERTY_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.linkTargetEntityIdPropertyName
import jetbrains.exodus.entitystore.orientdb.iterate.link.OVertexEntityIterable
import jetbrains.exodus.util.LightByteArrayOutputStream
import jetbrains.exodus.util.UTFUtil
import mu.KLogging
import java.io.ByteArrayInputStream
import java.io.File
import java.io.InputStream
import kotlin.jvm.optionals.getOrNull

open class OVertexEntity(internal val vertex: OVertex, private val store: OEntityStore) : OEntity {

    companion object : KLogging() {
        const val EDGE_CLASS_SUFFIX = "_link"
        private const val LINK_TARGET_ENTITY_ID_PROPERTY_NAME_SUFFIX = "_targetEntityId"
        private const val BLOB_SIZE_PROPERTY_NAME_SUFFIX = "_blob_size"
        private const val STRING_BLOB_HASH_PROPERTY_NAME_SUFFIX = "_string_blob_hash"
        fun blobSizeProperty(propertyName: String) = "\$$propertyName$BLOB_SIZE_PROPERTY_NAME_SUFFIX"
        fun blobHashProperty(propertyName: String) = "\$$propertyName$STRING_BLOB_HASH_PROPERTY_NAME_SUFFIX"

        // Backward compatible EntityId

        const val CLASS_ID_CUSTOM_PROPERTY_NAME = "classId"
        const val CLASS_ID_SEQUENCE_NAME = "sequence_classId"

        const val LOCAL_ENTITY_ID_PROPERTY_NAME = "localEntityId"
        fun localEntityIdSequenceName(className: String): String = "${className}_sequence_localEntityId"

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

    private val oEntityId = ORIDEntityId.fromVertex(vertex)

    override fun getStore() = store

    override fun getId(): OEntityId = oEntityId

    override fun toIdString(): String = oEntityId.toString()

    override fun getType(): String = oEntityId.getTypeName()

    override fun delete(): Boolean {
        requireActiveWritableTransaction()
        vertex.delete()
        return true
    }

    private fun requireActiveTx(): OStoreTransaction {
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
            OComparableSet(value)
        }
    }

    override fun setProperty(propertyName: String, value: Comparable<*>): Boolean {
        requireActiveWritableTransaction()
        val oldProperty = vertex.getProperty<Any>(propertyName)

        if (value is OComparableSet<*> || oldProperty is MutableSet<*>) {
            return setPropertyAsSet(propertyName, value as OComparableSet<*>)
        } else {
            vertex.setProperty(propertyName, value)
            vertex.save<OVertex>()
            return oldProperty?.equals(value) != true
        }
    }

    private fun setPropertyAsSet(propertyName: String, value: Any?): Boolean {
        if (value is OComparableSet<*>) {
            vertex.setProperty(propertyName, value.source)
        } else {
            vertex.setProperty(propertyName, value)
        }
        vertex.save<OVertex>()
        return vertex.getProperty<OTrackedSet<*>>(propertyName)?.isTransactionModified == true
    }

    override fun deleteProperty(propertyName: String): Boolean {
        requireActiveWritableTransaction()
        if (vertex.hasProperty(propertyName)) {
            vertex.removeProperty<Any>(propertyName)
            vertex.save<OVertex>()
            return true
        } else {
            return false
        }
    }

    override fun getPropertyNames(): List<String> {
        requireActiveTx()
        return ArrayList(vertex.propertyNames)
    }

    override fun getBlob(blobName: String): InputStream? {
        requireActiveTx()
        val blob: ORecordBytes = vertex.getProperty(blobName) ?: return null
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

        val oBlob = ORecordBytes()
        val size = oBlob.fromInputStream(blob)
        vertex.setProperty(blobName, oBlob)
        vertex.setProperty(blobSizeProperty(blobName), size.toLong())
        vertex.save<OVertex>()
    }

    override fun deleteBlob(blobName: String): Boolean {
        requireActiveWritableTransaction()
        if (vertex.hasProperty(blobName)) {
            vertex.removeProperty<Any>(blobName)
            vertex.removeProperty<Any>(blobSizeProperty(blobName))
            vertex.removeProperty<Any>(blobHashProperty(blobName))
            vertex.save<OVertex>()
            return true
        }
        return false
    }

    override fun getBlobString(blobName: String): String? {
        requireActiveTx()
        val blob: ORecordBytes = vertex.getProperty(blobName) ?: return null
        return UTFUtil.readUTF(ByteArrayInputStream(blob.toStream()))
    }

    override fun setBlob(blobName: String, file: File) {
        setBlob(blobName, file.inputStream())
    }

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

        val oBlob = ORecordBytes(baos.toByteArray())
        vertex.setProperty(blobName, oBlob)
        vertex.setProperty(blobHashProperty(blobName), blobString.hashCode())
        vertex.setProperty(blobSizeProperty(blobName), baos.size().toLong())
        vertex.save<OVertex>()
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
        requireActiveWritableTransaction()
        require(target is OVertexEntity) { "Only OVertexEntity is supported, but was ${target.javaClass.simpleName}" }
        return addLinkImpl(linkName, target.vertex)
    }

    override fun addLink(linkName: String, targetId: EntityId): Boolean {
        val currentTx = requireActiveWritableTransaction()
        val targetOId = store.requireOEntityId(targetId)
        if (targetOId == ORIDEntityId.EMPTY_ID) {
            return false
        }
        val target = currentTx.getRecord<OVertex>(targetOId.asOId()) ?: return false
        return addLinkImpl(linkName, target)
    }

    private fun addLinkImpl(linkName: String, target: OVertex): Boolean {
        val edgeClassName = edgeClassName(linkName)
        val edgeClass = ODatabaseSession.getActiveSession().getClass(edgeClassName) ?: throw IllegalStateException("$edgeClassName edge class not found in the database. Sorry, pal, it is required for adding a link.")

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
        val currentEdge: OEdge? = if (edgeClass.areIndexed(OEdge.DIRECTION_IN, OEdge.DIRECTION_OUT)) {
            findEdge(edgeClassName, target.identity)
        } else null

        if (currentEdge == null) {
            vertex.addEdge(target, edgeClassName)
            // If the link is indexed, we have to update the complementary internal property.
            vertex.addTargetEntityIdIfLinkIndexed(linkName, target.identity)
            vertex.save<OVertex>()
            return true
        } else {
            return false
        }
    }

    private fun OVertex.addTargetEntityIdIfLinkIndexed(linkName: String, targetId: ORID) {
        val linkTargetEntityIdPropertyName = linkTargetEntityIdPropertyName(linkName)
        if (requireSchemaClass().existsProperty(linkTargetEntityIdPropertyName)) {
            val bag = getProperty<ORidBag>(linkTargetEntityIdPropertyName) ?: ORidBag()
            bag.add(targetId)
            setProperty(linkTargetEntityIdPropertyName, bag)
        }
    }


    // Delete links

    override fun deleteLink(linkName: String, target: Entity): Boolean {
        requireActiveWritableTransaction()
        target as OVertexEntity
        val targetOId = target.oEntityId.asOId()
        return deleteLinkImpl(linkName, targetOId)
    }

    override fun deleteLink(linkName: String, targetId: EntityId): Boolean {
        requireActiveWritableTransaction()
        val targetOId = store.requireOEntityId(targetId).asOId()
        return deleteLinkImpl(linkName, targetOId)
    }

    override fun deleteLinks(linkName: String) {
        requireActiveWritableTransaction()
        val edgeClassName = edgeClassName(linkName)
        vertex.getEdges(ODirection.OUT, edgeClassName).forEach {
            it.delete()
        }
        vertex.deleteAllTargetEntityIdsIfLinkIndexed(linkName)
        vertex.save<OVertex>()
    }

    private fun deleteLinkImpl(linkName: String, targetId: ORID): Boolean {
        val edgeClassName = edgeClassName(linkName)

        val edge = findEdge(edgeClassName, targetId)
        if (edge != null) {
            edge.delete()
            // if the link in a composite index, we have to update the complementary internal property.
            vertex.deleteTargetEntityIdIfLinkIndexed(linkName, targetId)
            vertex.save<OVertex>()
            return true
        }

        return false
    }

    private fun OVertex.deleteTargetEntityIdIfLinkIndexed(linkName: String, targetId: ORID) {
        val linkTargetEntityIdPropertyName = linkTargetEntityIdPropertyName(linkName)
        if (requireSchemaClass().existsProperty(linkTargetEntityIdPropertyName)) {
            val bag = getProperty<ORidBag>(linkTargetEntityIdPropertyName) ?: ORidBag()
            bag.remove(targetId)
            setProperty(linkTargetEntityIdPropertyName, bag)
        }
    }

    private fun OVertex.deleteAllTargetEntityIdsIfLinkIndexed(linkName: String) {
        val propName = linkTargetEntityIdPropertyName(linkName)
        if (requireSchemaClass().existsProperty(propName)) {
            setProperty(propName, ORidBag())
        }
    }


    // Set links

    override fun setLink(linkName: String, target: Entity?): Boolean {
        requireActiveWritableTransaction()
        require(target is OVertexEntity?) { "Only OVertexEntity is supported, but was ${target?.javaClass?.simpleName}" }
        return setLinkImpl(linkName, target?.vertex)
    }

    override fun setLink(linkName: String, targetId: EntityId): Boolean {
        val currentTx = requireActiveWritableTransaction()
        val targetOId = store.requireOEntityId(targetId)
        if (targetOId == ORIDEntityId.EMPTY_ID) {
            return false
        }
        val target = currentTx.getRecord<OVertex>(targetOId.asOId()) ?: return false
        return setLinkImpl(linkName, target)
    }

    private fun setLinkImpl(linkName: String, target: OVertex?): Boolean {
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

    private fun getLinkImpl(linkName: String): OVertex? {
        val edgeClassName = edgeClassName(linkName)
        return vertex.getVertices(ODirection.OUT, edgeClassName).firstOrNull()
    }

    override fun getLinks(linkName: String): EntityIterable {
        requireActiveTx()
        val edgeClassName = edgeClassName(linkName)
        val links = vertex.getVertices(ODirection.OUT, edgeClassName)
        return OVertexEntityIterable(links, store)
    }

    override fun getLinks(linkNames: Collection<String>): EntityIterable {
        requireActiveTx()
        val edgeClassNames = linkNames.map { edgeClassName(it) }
        return OVertexEntityIterable(vertex.getVertices(ODirection.OUT, *edgeClassNames.toTypedArray()), store)
    }

    override fun getLinkNames(): List<String> {
        requireActiveTx()
        return ArrayList(vertex.getEdgeNames(ODirection.OUT)
            .filter { it.endsWith(EDGE_CLASS_SUFFIX) }
            .map { it.substringBefore(EDGE_CLASS_SUFFIX) })
    }

    private fun findEdge(edgeClassName: String, targetId: ORID): OEdge? {
        val query = "SELECT FROM $edgeClassName WHERE ${OEdge.DIRECTION_OUT} = :outId AND ${OEdge.DIRECTION_IN} = :inId"
        val result = ODatabaseSession.getActiveSession().query(query, mapOf("outId" to vertex.identity, "inId" to targetId))
        val foundEdge = result.edgeStream().findFirst()
        return foundEdge.getOrNull()
    }

    override fun compareTo(other: Entity) = id.compareTo(other.id)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is OEntity) return false
        if (javaClass != other.javaClass) return false

        return this.id == other.id
    }

    override fun hashCode(): Int {
        return id.hashCode()
    }

    override fun save(): OVertexEntity {
        requireActiveWritableTransaction()
        vertex.save<OVertex>()
        return this
    }

    protected open fun requireActiveWritableTransaction(): OStoreTransaction {
        return store.requireActiveWritableTransaction()
    }

    private fun OVertex?.toOEntityOrNull(): OEntity? = this?.let { OVertexEntity(this, store) }
}

fun OClass.requireClassId(): Int {
    return getCustom(CLASS_ID_CUSTOM_PROPERTY_NAME)?.toInt()
        ?: throw IllegalStateException("classId not found for ${this.name}")
}

fun OVertex.getTargetLocalEntityIds(linkName: String): ORidBag {
    return getProperty<ORidBag>(linkTargetEntityIdPropertyName(linkName)) ?: ORidBag()
}

fun OVertex.setTargetLocalEntityIds(linkName: String, ids: ORidBag) {
    setProperty(linkTargetEntityIdPropertyName(linkName), ids)
}

fun OVertex.requireSchemaClass(): OClass {
    return schemaClass ?: throw IllegalStateException("schemaClass not found for $this")
}

fun OVertex.requireLocalEntityId(): Long {
    return getProperty<Long>(LOCAL_ENTITY_ID_PROPERTY_NAME)
        ?: throw IllegalStateException("localEntityId not found for the vertex")
}


val String.asEdgeClass get() = OVertexEntity.edgeClassName(this)
