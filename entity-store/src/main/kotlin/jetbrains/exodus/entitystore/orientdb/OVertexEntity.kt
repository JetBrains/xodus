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
import com.orientechnologies.orient.core.id.ORID
import com.orientechnologies.orient.core.id.ORecordId
import com.orientechnologies.orient.core.record.ODirection
import com.orientechnologies.orient.core.record.OEdge
import com.orientechnologies.orient.core.record.OElement
import com.orientechnologies.orient.core.record.OVertex
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.entitystore.*
import jetbrains.exodus.entitystore.iterate.link.OEntityToLinksIterable
import mu.KLogging
import java.io.ByteArrayInputStream
import java.io.File
import java.io.InputStream

class OVertexEntity(private var vertex: OVertex) : OEntity {

    companion object : KLogging() {
        const val BINARY_BLOB_CLASS_NAME: String = "BinaryBlob"
        const val DATA_PROPERTY_NAME = "data"
        private const val BLOB_SIZE_PROPERTY_NAME_SUFFIX = "_blob_size"
        private const val STRING_BLOB_HASH_PROPERTY_NAME_SUFFIX = "_string_blob_hash"
        fun blobSizeProperty(propertyName: String) = "\$$propertyName$BLOB_SIZE_PROPERTY_NAME_SUFFIX"
        fun blobHashProperty(propertyName: String) = "\$$propertyName$STRING_BLOB_HASH_PROPERTY_NAME_SUFFIX"

        const val STRING_BLOB_CLASS_NAME: String = "StringBlob"
    }

    private val activeSession get() = ODatabaseSession.getActiveSession()

    private var txid: Int = activeSession.transaction.id

    override fun getStore(): EntityStore = throw UnsupportedOperationException()

    override fun getId(): EntityId = ORIDEntityId(vertex.identity)

    override fun getOId(): ORID = vertex.identity

    override fun toIdString(): String = vertex.identity.toString()

    override fun getType(): String = vertex.schemaClass!!.name

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
            vertex = session.load(vertex.identity)
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


    private fun createBlobElementWithNoContent(blobName: String, className: String): OElement {
        val ref = vertex.getLinkProperty(blobName)
        val record: OElement
        if (ref == null) {
            record = activeSession.newElement(className)
            vertex.setProperty(blobName, record)
        } else {
            record = activeSession.getRecord(ref)
            if (record.hasProperty(DATA_PROPERTY_NAME)) {
                record.removeProperty<Any>(DATA_PROPERTY_NAME)
            }
        }
        return record
    }

    override fun setBlob(blobName: String, blob: InputStream) {
        reload()
        val element = createBlobElementWithNoContent(blobName, BINARY_BLOB_CLASS_NAME)
        val data = blob.use { blob.readAllBytes() }
        element.setProperty(DATA_PROPERTY_NAME, data)
        vertex.setProperty(blobSizeProperty(blobName), data.size.toLong())
        element.save<OElement>()
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
            update = blobString.hashCode() != vertex.getProperty(blobHashProperty(blobName))
                    || blobString.length != vertex.getProperty(blobSizeProperty(blobName))
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
        val targetVertex = target.vertex
        //optimization?
        val currentEdge = findEdge(linkName, targetVertex)
        if (currentEdge == null) {
            vertex.addEdge(targetVertex, linkName)
            vertex.save<OVertex>()
            return true
        } else {
            return false
        }
    }

    override fun addLink(linkName: String, targetId: EntityId): Boolean {
        val target = activeSession.getRecord<OVertex>(ORecordId(targetId.typeId, targetId.localId))
        return addLink(linkName, OVertexEntity(target))
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
            findEdge(linkName, currentLink.vertex)?.delete()
            currentLink.vertex.save<OVertex>()
        }
        if (target != null) {
            vertex.addEdge(target.vertex, linkName)
            vertex.save<OVertex>()
        }
        return true
    }

    override fun setLink(linkName: String, targetId: EntityId): Boolean {
        val recordId = targetId.toRecordId()
        val target = activeSession.getRecord<OVertex>(recordId)
        return setLink(linkName, OVertexEntity(target))
    }

    override fun getLinks(linkName: String): EntityIterable {
        reload()
        val links = vertex.getVertices(ODirection.OUT, linkName)
        return OEntityToLinksIterable(links)
    }

    override fun getLinks(linkNames: Collection<String>): EntityIterable {
        reload()
        return OEntityToLinksIterable(vertex.getVertices(ODirection.OUT, *linkNames.toTypedArray()))
    }

    override fun deleteLink(linkName: String, target: Entity): Boolean {
        reload()
        target as OVertexEntity
        val currentEdge = findEdge(linkName, target.vertex)
        return if (currentEdge != null) {
            currentEdge.delete()
            target.vertex.save<OVertex>()
            vertex.save<OVertex>()
            true
        } else false
    }

    override fun deleteLink(linkName: String, targetId: EntityId): Boolean {
        val recordId = ORecordId(targetId.typeId, targetId.localId)
        val target = activeSession.getRecord<OVertex>(recordId)
        return deleteLink(linkName, OVertexEntity(target))
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

    private fun findEdge(linkName: String, target: OVertex): OEdge? {
        return vertex.getEdges(ODirection.OUT, linkName).find {
            it.to.identity == target.identity
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as OVertexEntity

        return vertex.identity == other.vertex.identity
    }

    override fun hashCode(): Int {
        return vertex.identity.hashCode()
    }

    internal val asVertex = vertex

    private fun OVertex?.toOEntityOrNull(): OEntity? = this?.let { OVertexEntity(this) }
}
