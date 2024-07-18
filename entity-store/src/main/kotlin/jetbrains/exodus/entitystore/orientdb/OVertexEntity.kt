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
import com.orientechnologies.orient.core.db.record.OIdentifiable
import com.orientechnologies.orient.core.db.record.OTrackedSet
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag
import com.orientechnologies.orient.core.id.ORID
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.record.ODirection
import com.orientechnologies.orient.core.record.OEdge
import com.orientechnologies.orient.core.record.OElement
import com.orientechnologies.orient.core.record.OVertex
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.EntityId
import jetbrains.exodus.entitystore.EntityIterable
import jetbrains.exodus.entitystore.PersistentEntityStore
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.CLASS_ID_CUSTOM_PROPERTY_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.LOCAL_ENTITY_ID_PROPERTY_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.linkTargetEntityIdPropertyName
import jetbrains.exodus.entitystore.orientdb.iterate.link.OVertexEntityIterable
import jetbrains.exodus.util.UTFUtil
import mu.KLogging
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.InputStream
import kotlin.jvm.optionals.getOrNull

open class OVertexEntity(internal val vertex: OVertex, private val store: PersistentEntityStore) : OEntity {

    companion object : KLogging() {
        const val BINARY_BLOB_CLASS_NAME: String = "BinaryBlob"
        const val DATA_PROPERTY_NAME = "data"
        const val EDGE_CLASS_SUFFIX = "_link"
        private const val LINK_TARGET_ENTITY_ID_PROPERTY_NAME_SUFFIX = "_targetEntityId"
        private const val BLOB_SIZE_PROPERTY_NAME_SUFFIX = "_blob_size"
        private const val STRING_BLOB_HASH_PROPERTY_NAME_SUFFIX = "_string_blob_hash"
        fun blobSizeProperty(propertyName: String) = "\$$propertyName$BLOB_SIZE_PROPERTY_NAME_SUFFIX"
        fun blobHashProperty(propertyName: String) = "\$$propertyName$STRING_BLOB_HASH_PROPERTY_NAME_SUFFIX"

        const val STRING_BLOB_CLASS_NAME: String = "StringBlob"

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

    private val activeSession get() = ODatabaseSession.getActiveSession()

    private var oEntityId = ORIDEntityId.fromVertex(vertex)

    override fun getStore() = store

    override fun getId(): OEntityId = oEntityId

    override fun toIdString(): String = oEntityId.toString()

    override fun getType(): String = oEntityId.getTypeName()

    override fun delete(): Boolean {
        vertex.delete()
        return true
    }

    override fun getRawProperty(propertyName: String): ByteIterable? = null

    override fun getProperty(propertyName: String): Comparable<*>? {
        val value = vertex.getProperty<Any>(propertyName)
        return if (value == null || value !is MutableSet<*>) {
            value as Comparable<*>?
        } else {
            OComparableSet(value)
        }
    }

    override fun setProperty(propertyName: String, value: Comparable<*>): Boolean {
        assertWritable()
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
        assertWritable()
        if (vertex.hasProperty(propertyName)) {
            vertex.removeProperty<Any>(propertyName)
            vertex.save<OVertex>()
            return true
        } else {
            return false
        }

    }

    override fun getPropertyNames(): List<String> {
        return ArrayList(vertex.propertyNames)
    }

    override fun getBlob(blobName: String): InputStream? {
        val element = vertex.getLinkProperty(blobName)
        return element?.let {
            val record = activeSession.getRecord<OElement>(element)
            return ByteArrayInputStream(record.getProperty(DATA_PROPERTY_NAME))
        }
    }

    override fun getBlobSize(blobName: String): Long {
        val sizePropertyName = blobSizeProperty(blobName)
        val ref = vertex.getLinkProperty(blobName)
        return ref?.let {
            vertex.getProperty<Long>(sizePropertyName)
        } ?: -1
    }

    override fun getBlobString(blobName: String): String? {
        val ref = vertex.getLinkProperty(blobName)
        return ref?.let {
            val record = activeSession.getRecord<OElement>(ref)
            record.getProperty<ByteArray>(DATA_PROPERTY_NAME)?.let {
                UTFUtil.readUTF(ByteArrayInputStream(it))
            }
        }
    }


    override fun setBlob(blobName: String, blob: InputStream) {
        assertWritable()
        val ref = vertex.getLinkProperty(blobName)
        val blobContainer: OElement

        if (ref == null) {
            blobContainer = activeSession.newElement(BINARY_BLOB_CLASS_NAME)
            vertex.setProperty(blobName, blobContainer)
        } else {
            blobContainer = activeSession.getRecord(ref)
            if (blobContainer.hasProperty(DATA_PROPERTY_NAME)) {
                blobContainer.removeProperty<Any>(DATA_PROPERTY_NAME)
            }
        }

        val data = blob.use { blob.readAllBytes() }
        blobContainer.setProperty(DATA_PROPERTY_NAME, data)
        vertex.setProperty(blobSizeProperty(blobName), data.size.toLong())
        blobContainer.save<OElement>()
        vertex.save<OVertex>()
    }

    override fun setBlob(blobName: String, file: File) {
        setBlob(blobName, file.inputStream())
    }

    override fun setBlobString(blobName: String, blobString: String): Boolean {
        assertWritable()
        val ref = vertex.getLinkProperty(blobName)
        val update: Boolean
        var record: OElement? = null

        if (ref == null) {
            record = activeSession.newElement(STRING_BLOB_CLASS_NAME)
            vertex.setProperty(blobName, record)
            update = true
        } else {
            update = blobString.hashCode() != vertex.getProperty<Int>(blobHashProperty(blobName))
                    || blobString.length.toLong() != vertex.getProperty<Long>(blobSizeProperty(blobName))
        }

        if (update) {
            record = record ?: activeSession.getRecord(ref) as OElement
            vertex.setProperty(blobHashProperty(blobName), blobString.hashCode())
            vertex.setProperty(blobSizeProperty(blobName), blobString.length.toLong())
            val baos = ByteArrayOutputStream(blobString.length)
            UTFUtil.writeUTF(baos, blobString)
            record.setProperty(DATA_PROPERTY_NAME, baos.toByteArray())
            vertex.save<OVertex>()
        }

        return update
    }

    override fun deleteBlob(blobName: String): Boolean {
        assertWritable()
        val ref = vertex.getLinkProperty(blobName)
        return if (ref != null) {
            val record = ref.getRecord<OElement>()
            vertex.removeProperty<Long>(blobSizeProperty(blobName))
            if (record.schemaClass?.name == STRING_BLOB_CLASS_NAME) {
                record.setProperty(DATA_PROPERTY_NAME, null)
            } else {
                vertex.removeProperty<OIdentifiable>(blobName)
                vertex.save<OVertex>()
                activeSession.delete(ref.identity)
            }
            true
        } else false
    }

    override fun getBlobNames(): List<String> {
        return vertex.propertyNames
            .filter { it.endsWith(BLOB_SIZE_PROPERTY_NAME_SUFFIX) }
            .map { it.substring(1).substringBefore(BLOB_SIZE_PROPERTY_NAME_SUFFIX) }
    }

    // Add links

    override fun addLink(linkName: String, target: Entity): Boolean {
        assertWritable()
        require(target is OVertexEntity) { "Only OVertexEntity is supported, but was ${target.javaClass.simpleName}" }
        return addLinkImpl(linkName, target.vertex)
    }

    override fun addLink(linkName: String, targetId: EntityId): Boolean {
        assertWritable()
        val targetOId = store.requireOEntityId(targetId)
        if (targetOId == ORIDEntityId.EMPTY_ID) {
            return false
        }
        val target = activeSession.getRecord<OVertex>(targetOId.asOId()) ?: return false
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
        assertWritable()
        target as OVertexEntity
        val targetOId = target.oEntityId.asOId()
        return deleteLinkImpl(linkName, targetOId)
    }

    override fun deleteLink(linkName: String, targetId: EntityId): Boolean {
        assertWritable()
        val targetOId = store.requireOEntityId(targetId).asOId()
        return deleteLinkImpl(linkName, targetOId)
    }

    override fun deleteLinks(linkName: String) {
        assertWritable()
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
            edge.save<OEdge>()
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
        assertWritable()
        require(target is OVertexEntity?) { "Only OVertexEntity is supported, but was ${target?.javaClass?.simpleName}" }
        return setLinkImpl(linkName, target?.vertex)
    }

    override fun setLink(linkName: String, targetId: EntityId): Boolean {
        assertWritable()
        val targetOId = store.requireOEntityId(targetId)
        if (targetOId == ORIDEntityId.EMPTY_ID) {
            return false
        }
        val target = activeSession.getRecord<OVertex>(targetOId.asOId()) ?: return false
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
        return getLinkImpl(linkName).toOEntityOrNull()
    }

    private fun getLinkImpl(linkName: String): OVertex? {
        val edgeClassName = edgeClassName(linkName)
        return vertex.getVertices(ODirection.OUT, edgeClassName).firstOrNull()
    }

    override fun getLinks(linkName: String): EntityIterable {
        val edgeClassName = edgeClassName(linkName)
        val links = vertex.getVertices(ODirection.OUT, edgeClassName)
        return OVertexEntityIterable(links, store)
    }

    override fun getLinks(linkNames: Collection<String>): EntityIterable {
        val edgeClassNames = linkNames.map { edgeClassName(it) }
        return OVertexEntityIterable(vertex.getVertices(ODirection.OUT, *edgeClassNames.toTypedArray()), store)
    }

    override fun getLinkNames(): List<String> {
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
        assertWritable()
        vertex.save<OVertex>()
        return this
    }

    protected open fun assertWritable() {}

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
