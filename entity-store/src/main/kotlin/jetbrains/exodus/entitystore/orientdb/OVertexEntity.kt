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
import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.db.record.OIdentifiable
import com.orientechnologies.orient.core.id.ORecordId
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.metadata.sequence.OSequence
import com.orientechnologies.orient.core.record.ODirection
import com.orientechnologies.orient.core.record.OEdge
import com.orientechnologies.orient.core.record.OElement
import com.orientechnologies.orient.core.record.OVertex
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.entitystore.*
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.LOCAL_ENTITY_ID_PROPERTY_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.CLASS_ID_CUSTOM_PROPERTY_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.CLASS_ID_SEQUENCE_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.localEntityIdSequenceName
import jetbrains.exodus.entitystore.orientdb.iterate.link.OVertexEntityIterable
import mu.KLogging
import java.io.ByteArrayInputStream
import java.io.File
import java.io.InputStream

class OVertexEntity(private var vertex: OVertex, private val store: PersistentEntityStore) : OEntity {

    companion object : KLogging() {
        const val BINARY_BLOB_CLASS_NAME: String = "BinaryBlob"
        const val DATA_PROPERTY_NAME = "data"
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
        return false
    }

    override fun getRawProperty(propertyName: String): ByteIterable? = null

    override fun getProperty(propertyName: String): Comparable<*>? {
        reload()
        return vertex.getProperty<Comparable<*>>(propertyName)
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
        reload()
        val oldProperty = vertex.getProperty<Comparable<*>>(propertyName)
        vertex.setProperty(propertyName, value)
        vertex.save<OVertex>()
        return oldProperty?.equals(value) != true
    }

    override fun deleteProperty(propertyName: String): Boolean {
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
            record.getProperty(DATA_PROPERTY_NAME)
        }
    }


    override fun setBlob(blobName: String, blob: InputStream) {
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
            record.setProperty(DATA_PROPERTY_NAME, blobString)
            vertex.save<OVertex>()
        }

        return update
    }

    override fun deleteBlob(blobName: String): Boolean {
        reload()
        val ref = vertex.getLinkProperty(blobName)
        return if (ref != null) {
            //todo check if remove is norm
            vertex.removeProperty<OIdentifiable>(blobName)
            vertex.removeProperty<Long>(blobSizeProperty(blobName))
            vertex.save<OVertex>()
            activeSession.delete(ref.identity)
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
        reload()
        require(target is OVertexEntity) { "Only OVertexEntity is supported, but was ${target.javaClass.simpleName}" }
        // Optimization?
        val currentEdge = findEdge(linkName, target.id)
        if (currentEdge == null) {
            vertex.addEdge(target.asVertex, linkName)
            vertex.save<OVertex>()
            return true
        } else {
            return false
        }
    }

    override fun addLink(linkName: String, targetId: EntityId): Boolean {
        val targetOId = store.requireOEntityId(targetId)
        if (targetOId == ORIDEntityId.EMPTY_ID) {
            return false
        }
        val target = activeSession.getRecord<OVertex>(targetOId.asOId()) ?: return false
        return addLink(linkName, OVertexEntity(target, store))
    }

    override fun getLink(linkName: String): Entity? {
        reload()
        val target = vertex.getVertices(ODirection.OUT, linkName).firstOrNull()
        return target.toOEntityOrNull()
    }

    override fun setLink(linkName: String, target: Entity?): Boolean {
        require(target is OVertexEntity?) { "Only OVertexEntity is supported, but was ${target?.javaClass?.simpleName}" }

        reload()
        val currentLink = getLink(linkName) as OVertexEntity?
        if (currentLink == target) {
            return false
        }
        if (currentLink != null) {
            findEdge(linkName, currentLink.id)?.delete()
            currentLink.vertex.save<OVertex>()
        }
        if (target != null) {
            vertex.addEdge(target.vertex, linkName)
            vertex.save<OVertex>()
        }
        return true
    }

    override fun setLink(linkName: String, targetId: EntityId): Boolean {
        val targetOId = store.requireOEntityId(targetId)
        if (targetOId == ORIDEntityId.EMPTY_ID) {
            return false
        }
        val target = activeSession.getRecord<OVertex>(targetOId.asOId()) ?: return false
        return setLink(linkName, OVertexEntity(target, store))
    }

    override fun getLinks(linkName: String): EntityIterable {
        reload()
        val links = vertex.getVertices(ODirection.OUT, linkName)
        return OVertexEntityIterable(links, store)
    }

    override fun getLinks(linkNames: Collection<String>): EntityIterable {
        reload()
        return OVertexEntityIterable(vertex.getVertices(ODirection.OUT, *linkNames.toTypedArray()), store)
    }

    override fun deleteLink(linkName: String, target: Entity): Boolean {
        reload()
        target as OVertexEntity
        vertex.deleteEdge(target.vertex, linkName)
        val result = vertex.isDirty
        vertex.save<OVertex>()
        return result
    }

    override fun deleteLink(linkName: String, targetId: EntityId): Boolean {
        val recordId = ORecordId(targetId.typeId, targetId.localId)
        val target = activeSession.getRecord<OVertex>(recordId)
        return deleteLink(linkName, OVertexEntity(target, store))
    }

    override fun deleteLinks(linkName: String) {
        reload()
        vertex.getEdges(ODirection.OUT, linkName).forEach {
            it.delete()
        }
        vertex.save<OVertex>()
    }

    override fun getLinkNames(): List<String> {
        reload()
        return ArrayList(vertex.getEdgeNames(ODirection.OUT))
    }

    override fun compareTo(other: Entity) = id.compareTo(other.id)

    private fun findEdge(linkName: String, targetId: OEntityId): OEdge? {
        return vertex
            .getEdges(ODirection.OUT, linkName)
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
        vertex.save<OVertex>()
        return this
    }

    private fun OVertex?.toOEntityOrNull(): OEntity? = this?.let { OVertexEntity(this, store) }
}

fun ODatabaseSession.createClassIdSequenceIfAbsent(startFrom: Long = 0L) {
    createSequenceIfAbsent(CLASS_ID_SEQUENCE_NAME, startFrom)
}

fun ODatabaseSession.createLocalEntityIdSequenceIfAbsent(oClass: OClass, startFrom: Long = 0L) {
    createSequenceIfAbsent(localEntityIdSequenceName(oClass.name), startFrom)
}

fun ODatabaseSession.createSequenceIfAbsent(sequenceName: String, startFrom: Long = 0L) {
    val sequences = metadata.sequenceLibrary
    if (sequences.getSequence(sequenceName) == null) {
        val params = OSequence.CreateParams()
        params.start = startFrom
        sequences.createSequence(sequenceName, OSequence.SEQUENCE_TYPE.ORDERED, params)
    }
}

fun ODatabaseSession.setClassIdIfAbsent(oClass: OClass) {
    if (oClass.getCustom(CLASS_ID_CUSTOM_PROPERTY_NAME) == null) {
        val sequences = metadata.sequenceLibrary
        val sequence: OSequence = sequences.getSequence(CLASS_ID_SEQUENCE_NAME) ?: throw IllegalStateException("$CLASS_ID_SEQUENCE_NAME not found")

        oClass.setCustom(CLASS_ID_CUSTOM_PROPERTY_NAME, sequence.next().toString())
    }
}

fun ODatabaseSession.setLocalEntityIdIfAbsent(vertex: OVertex) {
    if (vertex.getProperty<Long>(LOCAL_ENTITY_ID_PROPERTY_NAME) == null) {
        val oClass = vertex.requireSchemaClass()
        setLocalEntityId(oClass.name, vertex)
    }
}

fun ODatabaseDocument.setLocalEntityId(className: String, vertex: OVertex) {
    val sequences = metadata.sequenceLibrary
    val sequenceName = localEntityIdSequenceName(className)
    val sequence: OSequence = sequences.getSequence(sequenceName) ?: throw IllegalStateException("$sequenceName not found")
    vertex.setProperty(LOCAL_ENTITY_ID_PROPERTY_NAME, sequence.next())
}

fun ODatabaseSession.getOrCreateVertexClass(className: String): OClass {
    val existingClass = this.getClass(className)
    if (existingClass != null) return existingClass

    createClassIdSequenceIfAbsent()
    val oClass = createVertexClass(className)
    setClassIdIfAbsent(oClass)
    createLocalEntityIdSequenceIfAbsent(oClass)
    return oClass
}

fun OClass.requireClassId(): Int {
    return getCustom(CLASS_ID_CUSTOM_PROPERTY_NAME)?.toInt() ?: throw IllegalStateException("classId not found for ${this.name}")
}

fun OVertex.requireSchemaClass(): OClass {
    return schemaClass ?: throw IllegalStateException("schemaClass not found for $this")
}

fun OVertex.requireLocalEntityId(): Long {
    return getProperty<Long>(LOCAL_ENTITY_ID_PROPERTY_NAME) ?: throw IllegalStateException("localEntityId not found for the vertex")
}