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
import com.orientechnologies.orient.core.id.ORecordId
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
import jetbrains.exodus.entitystore.orientdb.iterate.link.OVertexEntityIterable
import jetbrains.exodus.util.UTFUtil
import mu.KLogging
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.InputStream

open class OVertexEntity(private var vertex: OVertex, private val store: PersistentEntityStore) : OEntity {

    companion object : KLogging() {
        const val BINARY_BLOB_CLASS_NAME: String = "BinaryBlob"
        const val DATA_PROPERTY_NAME = "data"
        const val EDGE_CLASS_SUFFIX = "\$link"
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
    }

    private val activeSession get() = ODatabaseSession.getActiveSession()

    private var txid: Int = activeSession.transaction.id

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
        reload()
        val value = vertex.getProperty<Any>(propertyName)
        return if (value == null || value !is MutableSet<*>) {
            value as Comparable<*>?
        } else {
            OComparableSet(value)
        }
    }

    private fun reload() {
        val session = activeSession
        val tx = session.transaction

        if (txid != tx.id) {
            txid = tx.id
            vertex = session.load(oEntityId.asOId())
        }
    }

    override fun setProperty(propertyName: String, value: Comparable<*>): Boolean {
        assertWritable()
        reload()
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
        reload()
        if (vertex.hasProperty(propertyName)) {
            vertex.removeProperty<Any>(propertyName)
            vertex.save<OVertex>()
            return true
        } else {
            return false
        }

    }

    override fun getPropertyNames(): List<String> {
        reload()
        return ArrayList(vertex.propertyNames)
    }

    override fun getBlob(blobName: String): InputStream? {
        reload()
        val element = vertex.getLinkProperty(blobName)
        return element?.let {
            val record = activeSession.getRecord<OElement>(element)
            return ByteArrayInputStream(record.getProperty(DATA_PROPERTY_NAME))
        }
    }

    override fun getBlobSize(blobName: String): Long {
        reload()
        val sizePropertyName = blobSizeProperty(blobName)
        val ref = vertex.getLinkProperty(blobName)
        return ref?.let {
            vertex.getProperty<Long>(sizePropertyName)
        } ?: -1
    }

    override fun getBlobString(blobName: String): String? {
        reload()
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
        reload()
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
        reload()
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
        reload()
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
        reload()
        return vertex.propertyNames
            .filter { it.endsWith(BLOB_SIZE_PROPERTY_NAME_SUFFIX) }
            .map { it.substring(1).substringBefore(BLOB_SIZE_PROPERTY_NAME_SUFFIX) }
    }

    override fun addLink(linkName: String, target: Entity): Boolean {
        assertWritable()
        reload()
        require(target is OVertexEntity) { "Only OVertexEntity is supported, but was ${target.javaClass.simpleName}" }
        // Optimization?
        val edgeClassName = edgeClassName(linkName)
        val currentEdge = findEdge(edgeClassName, target.id)
        if (currentEdge == null) {
            vertex.addEdge(target.asVertex, edgeClassName)
            vertex.save<OVertex>()
            return true
        } else {
            return false
        }
    }

    override fun addLink(linkName: String, targetId: EntityId): Boolean {
        assertWritable()
        val targetOId = store.requireOEntityId(targetId)
        if (targetOId == ORIDEntityId.EMPTY_ID) {
            return false
        }
        val target = activeSession.getRecord<OVertex>(targetOId.asOId()) ?: return false
        return addLink(linkName, OVertexEntity(target, store))
    }

    override fun getLink(linkName: String): Entity? {
        reload()
        val edgeClassName = edgeClassName(linkName)
        val target = vertex.getVertices(ODirection.OUT, edgeClassName).firstOrNull()
        return target.toOEntityOrNull()
    }

    override fun setLink(linkName: String, target: Entity?): Boolean {
        assertWritable()
        require(target is OVertexEntity?) { "Only OVertexEntity is supported, but was ${target?.javaClass?.simpleName}" }

        reload()
        val currentLink = getLink(linkName) as OVertexEntity?
        val edgeClassName = edgeClassName(linkName)

        if (currentLink == target) {
            return false
        }
        if (currentLink != null) {
            findEdge(edgeClassName, currentLink.id)?.delete()
            currentLink.vertex.save<OVertex>()
        }
        if (target != null) {
            vertex.addEdge(target.vertex, edgeClassName)
            vertex.save<OVertex>()
        }
        return true
    }

    override fun setLink(linkName: String, targetId: EntityId): Boolean {
        assertWritable()
        val targetOId = store.requireOEntityId(targetId)
        if (targetOId == ORIDEntityId.EMPTY_ID) {
            return false
        }
        val target = activeSession.getRecord<OVertex>(targetOId.asOId()) ?: return false
        return setLink(linkName, OVertexEntity(target, store))
    }

    override fun getLinks(linkName: String): EntityIterable {
        reload()
        val edgeClassName = edgeClassName(linkName)
        val links = vertex.getVertices(ODirection.OUT, edgeClassName)
        return OVertexEntityIterable(links, store)
    }

    override fun getLinks(linkNames: Collection<String>): EntityIterable {
        reload()
        val edgeClassNames = linkNames.map { edgeClassName(it) }
        return OVertexEntityIterable(vertex.getVertices(ODirection.OUT, *edgeClassNames.toTypedArray()), store)
    }

    override fun deleteLink(linkName: String, target: Entity): Boolean {
        assertWritable()
        reload()
        target as OVertexEntity
        val edgeClassName = edgeClassName(linkName)
        vertex.deleteEdge(target.vertex, edgeClassName)
        val result = vertex.isDirty
        vertex.save<OVertex>()
        return result
    }

    override fun deleteLink(linkName: String, targetId: EntityId): Boolean {
        assertWritable()
        val recordId = ORecordId(targetId.typeId, targetId.localId)
        val target = activeSession.getRecord<OVertex>(recordId)
        return deleteLink(linkName, OVertexEntity(target, store))
    }

    override fun deleteLinks(linkName: String) {
        assertWritable()
        reload()
        val edgeClassName = edgeClassName(linkName)
        vertex.getEdges(ODirection.OUT, edgeClassName).forEach {
            it.delete()
        }
        vertex.save<OVertex>()
    }

    override fun getLinkNames(): List<String> {
        reload()
        return ArrayList(vertex.getEdgeNames(ODirection.OUT)
            .filter { it.endsWith(EDGE_CLASS_SUFFIX) }
            .map { it.substringBefore(EDGE_CLASS_SUFFIX) })
    }

    override fun compareTo(other: Entity) = id.compareTo(other.id)

    private fun findEdge(edgeClassName: String, targetId: OEntityId): OEdge? {
        return vertex
            .getEdges(ODirection.OUT, edgeClassName)
            .find { it.to.identity == targetId.asOId() }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is OEntity) return false
        if (javaClass != other.javaClass) return false

        return this.id == other.id
    }

    override fun hashCode(): Int {
        return id.hashCode()
    }

    internal val asVertex = vertex

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

fun OVertex.requireSchemaClass(): OClass {
    return schemaClass ?: throw IllegalStateException("schemaClass not found for $this")
}

fun OVertex.requireLocalEntityId(): Long {
    return getProperty<Long>(LOCAL_ENTITY_ID_PROPERTY_NAME)
        ?: throw IllegalStateException("localEntityId not found for the vertex")
}


val String.asEdgeClass get() = OVertexEntity.edgeClassName(this)
