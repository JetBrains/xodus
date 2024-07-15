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
import com.orientechnologies.orient.core.id.ORecordId
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.record.ODirection
import com.orientechnologies.orient.core.record.OEdge
import com.orientechnologies.orient.core.record.OElement
import com.orientechnologies.orient.core.record.OVertex
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.entitystore.*
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.CLASS_ID_CUSTOM_PROPERTY_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.LOCAL_ENTITY_ID_PROPERTY_NAME
import jetbrains.exodus.entitystore.orientdb.iterate.link.OVertexEntityIterable
import jetbrains.exodus.util.UTFUtil
import mu.KLogging
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.InputStream
import kotlin.jvm.optionals.getOrNull

open class OVertexEntity(private var oEntityId: ORIDEntityId, private val store: PersistentEntityStore) : OEntity {

    constructor(vertex: OVertex, store: PersistentEntityStore) : this(ORIDEntityId.fromVertex(vertex), store) {
        this.vertex = vertex
    }

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

    private var vertex: OVertex? = null

    private val dbVertex: OVertex
        get() {
            if (vertex == null) {
                vertex = activeSession.getRecord(oEntityId.asOId())
            }

            if (vertex == null) {
                throw EntityRemovedInDatabaseException("Vertex not found for $oEntityId")
            }

            return vertex!!
        }

    override fun getStore() = store

    override fun getId(): OEntityId = oEntityId

    override fun toIdString(): String = oEntityId.toString()

    override fun getType(): String = oEntityId.getTypeName()

    override fun delete(): Boolean {
        dbVertex.delete()
        return true
    }

    override fun getRawProperty(propertyName: String): ByteIterable? = null

    override fun getProperty(propertyName: String): Comparable<*>? {
        val value = dbVertex.getProperty<Any>(propertyName)
        return if (value == null || value !is MutableSet<*>) {
            value as Comparable<*>?
        } else {
            OComparableSet(value)
        }
    }

    override fun setProperty(propertyName: String, value: Comparable<*>): Boolean {
        assertWritable()

        val dbVertex = this.dbVertex
        val oldProperty = dbVertex.getProperty<Any>(propertyName)

        if (value is OComparableSet<*> || oldProperty is MutableSet<*>) {
            return setPropertyAsSet(propertyName, value as OComparableSet<*>)
        } else {
            dbVertex.setProperty(propertyName, value)
            dbVertex.save<OVertex>()
            return oldProperty?.equals(value) != true
        }
    }

    private fun setPropertyAsSet(propertyName: String, value: Any?): Boolean {
        val dbVertex = this.dbVertex

        if (value is OComparableSet<*>) {
            dbVertex.setProperty(propertyName, value.source)
        } else {
            dbVertex.setProperty(propertyName, value)
        }
        dbVertex.save<OVertex>()
        return dbVertex.getProperty<OTrackedSet<*>>(propertyName)?.isTransactionModified == true
    }

    override fun deleteProperty(propertyName: String): Boolean {
        assertWritable()

        val dbVertex = this.dbVertex
        if (dbVertex.hasProperty(propertyName)) {
            dbVertex.removeProperty<Any>(propertyName)
            dbVertex.save<OVertex>()
            return true
        } else {
            return false
        }

    }

    override fun getPropertyNames(): List<String> {
        return ArrayList(dbVertex.propertyNames)
    }

    override fun getBlob(blobName: String): InputStream? {
        val element = dbVertex.getLinkProperty(blobName)
        return element?.let {
            val record = activeSession.getRecord<OElement>(element)
            return ByteArrayInputStream(record.getProperty(DATA_PROPERTY_NAME))
        }
    }

    override fun getBlobSize(blobName: String): Long {
        val dbVertex = this.dbVertex
        val sizePropertyName = blobSizeProperty(blobName)
        val ref = dbVertex.getLinkProperty(blobName)
        return ref?.let {
            dbVertex.getProperty(sizePropertyName)
        } ?: -1
    }

    override fun getBlobString(blobName: String): String? {
        val ref = dbVertex.getLinkProperty(blobName)
        return ref?.let {
            val record = activeSession.getRecord<OElement>(ref)
            record.getProperty<ByteArray>(DATA_PROPERTY_NAME)?.let {
                UTFUtil.readUTF(ByteArrayInputStream(it))
            }
        }
    }


    override fun setBlob(blobName: String, blob: InputStream) {
        assertWritable()
        val dbVertex = this.dbVertex

        val ref = dbVertex.getLinkProperty(blobName)
        val blobContainer: OElement

        if (ref == null) {
            blobContainer = activeSession.newElement(BINARY_BLOB_CLASS_NAME)
            dbVertex.setProperty(blobName, blobContainer)
        } else {
            blobContainer = activeSession.getRecord(ref)
            if (blobContainer.hasProperty(DATA_PROPERTY_NAME)) {
                blobContainer.removeProperty<Any>(DATA_PROPERTY_NAME)
            }
        }

        val data = blob.use { blob.readAllBytes() }
        blobContainer.setProperty(DATA_PROPERTY_NAME, data)
        dbVertex.setProperty(blobSizeProperty(blobName), data.size.toLong())
        blobContainer.save<OElement>()
        dbVertex.save<OVertex>()
    }

    override fun setBlob(blobName: String, file: File) {
        setBlob(blobName, file.inputStream())
    }

    override fun setBlobString(blobName: String, blobString: String): Boolean {
        assertWritable()

        val dbVertex = this.dbVertex
        val ref = dbVertex.getLinkProperty(blobName)
        val update: Boolean
        var record: OElement? = null

        if (ref == null) {
            record = activeSession.newElement(STRING_BLOB_CLASS_NAME)
            dbVertex.setProperty(blobName, record)
            update = true
        } else {
            update = blobString.hashCode() != dbVertex.getProperty<Int>(blobHashProperty(blobName))
                    || blobString.length.toLong() != dbVertex.getProperty<Long>(blobSizeProperty(blobName))
        }

        if (update) {
            record = record ?: activeSession.getRecord(ref) as OElement
            dbVertex.setProperty(blobHashProperty(blobName), blobString.hashCode())
            dbVertex.setProperty(blobSizeProperty(blobName), blobString.length.toLong())
            val baos = ByteArrayOutputStream(blobString.length)
            UTFUtil.writeUTF(baos, blobString)
            record.setProperty(DATA_PROPERTY_NAME, baos.toByteArray())
            dbVertex.save<OVertex>()
        }

        return update
    }

    override fun deleteBlob(blobName: String): Boolean {
        assertWritable()

        val dbVertex = this.dbVertex
        val ref = dbVertex.getLinkProperty(blobName)
        return if (ref != null) {
            val record = ref.getRecord<OElement>()
            dbVertex.removeProperty<Long>(blobSizeProperty(blobName))
            if (record.schemaClass?.name == STRING_BLOB_CLASS_NAME) {
                record.setProperty(DATA_PROPERTY_NAME, null)
            } else {
                dbVertex.removeProperty<OIdentifiable>(blobName)
                dbVertex.save<OVertex>()
                activeSession.delete(ref.identity)
            }
            true
        } else false
    }

    override fun getBlobNames(): List<String> {
        return dbVertex.propertyNames
            .filter { it.endsWith(BLOB_SIZE_PROPERTY_NAME_SUFFIX) }
            .map { it.substring(1).substringBefore(BLOB_SIZE_PROPERTY_NAME_SUFFIX) }
    }

    override fun addLink(linkName: String, target: Entity): Boolean {
        assertWritable()

        require(target is OVertexEntity) { "Only OVertexEntity is supported, but was ${target.javaClass.simpleName}" }
        // Optimization?
        val edgeClassName = edgeClassName(linkName)
        val edgeClass = ODatabaseSession.getActiveSession().getClass(edgeClassName)
            ?: throw IllegalStateException("$edgeClassName edge class not found in the database. Sorry, pal, it is required for adding a link.")

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
            findEdge(edgeClassName, target.id)
        } else null

        val vertex = this.dbVertex
        if (currentEdge == null) {
            vertex.addEdge(target.asVertex, edgeClassName)
            // If the link is indexed, we have to update the complementary internal property.
            val linkTargetEntityIdPropertyName = linkTargetEntityIdPropertyName(linkName)
            if (vertex.requireSchemaClass().existsProperty(linkTargetEntityIdPropertyName)) {
                val bag = vertex.getProperty<ORidBag>(linkTargetEntityIdPropertyName) ?: ORidBag()
                bag.add(target.asVertex)
                vertex.setProperty(linkTargetEntityIdPropertyName, bag)
            }
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
        val edgeClassName = edgeClassName(linkName)
        val target = dbVertex.getVertices(ODirection.OUT, edgeClassName).firstOrNull()
        return target.toOEntityOrNull()
    }

    override fun setLink(linkName: String, target: Entity?): Boolean {
        assertWritable()
        require(target is OVertexEntity?) { "Only OVertexEntity is supported, but was ${target?.javaClass?.simpleName}" }

        val currentLink = getLink(linkName) as OVertexEntity?
        val edgeClassName = edgeClassName(linkName)

        if (currentLink == target) {
            return false
        }

        val dbVertex = this.dbVertex
        if (currentLink != null) {
            findEdge(edgeClassName, currentLink.id)?.delete()
            currentLink.dbVertex.save<OVertex>()
        }
        if (target != null) {
            dbVertex.addEdge(target.vertex, edgeClassName)
            dbVertex.save<OVertex>()
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
        val edgeClassName = edgeClassName(linkName)
        val links = dbVertex.getVertices(ODirection.OUT, edgeClassName)
        return OVertexEntityIterable(links, store)
    }

    override fun getLinks(linkNames: Collection<String>): EntityIterable {
        val edgeClassNames = linkNames.map { edgeClassName(it) }
        return OVertexEntityIterable(dbVertex.getVertices(ODirection.OUT, *edgeClassNames.toTypedArray()), store)
    }

    override fun deleteLink(linkName: String, target: Entity): Boolean {
        assertWritable()
        target as OVertexEntity
        val edgeClassName = edgeClassName(linkName)

        val vertex = this.dbVertex
        vertex.deleteEdge(target.vertex, edgeClassName)
        val result = vertex.isDirty

        // if the link in a composite index, we have to update the complementary internal property.
        val linkTargetEntityIdPropertyName = linkTargetEntityIdPropertyName(linkName)
        if (vertex.requireSchemaClass().existsProperty(linkTargetEntityIdPropertyName)) {
            val bag = vertex.getProperty<ORidBag>(linkTargetEntityIdPropertyName) ?: ORidBag()
            bag.remove(target.vertex)
            vertex.setProperty(linkTargetEntityIdPropertyName, bag)
        }

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
        val edgeClassName = edgeClassName(linkName)

        val dbVertex = this.dbVertex
        dbVertex.getEdges(ODirection.OUT, edgeClassName).forEach {
            it.delete()
        }
        dbVertex.save<OVertex>()
    }

    override fun getLinkNames(): List<String> {
        return ArrayList(dbVertex.getEdgeNames(ODirection.OUT)
            .filter { it.endsWith(EDGE_CLASS_SUFFIX) }
            .map { it.substringBefore(EDGE_CLASS_SUFFIX) })
    }

    override fun compareTo(other: Entity) = id.compareTo(other.id)

    private fun findEdge(edgeClassName: String, targetId: OEntityId): OEdge? {
        val query = "SELECT FROM $edgeClassName WHERE ${OEdge.DIRECTION_OUT} = :outId AND ${OEdge.DIRECTION_IN} = :inId"
        val result = ODatabaseSession.getActiveSession()
            .query(query, mapOf("outId" to dbVertex.identity, "inId" to targetId.asOId()))
        val foundEdge = result.edgeStream().findFirst()
        return foundEdge.getOrNull()
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

    internal val asVertex = dbVertex

    override fun save(): OVertexEntity {
        assertWritable()
        dbVertex.save<OVertex>()
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
