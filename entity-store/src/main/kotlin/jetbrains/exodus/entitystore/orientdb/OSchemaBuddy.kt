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

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal
import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.metadata.sequence.OSequence
import com.orientechnologies.orient.core.metadata.sequence.OSequence.CreateParams
import com.orientechnologies.orient.core.metadata.sequence.OSequence.SEQUENCE_TYPE
import com.orientechnologies.orient.core.record.OVertex
import com.orientechnologies.orient.core.sql.executor.OResultSet
import jetbrains.exodus.entitystore.PersistentEntityId
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.CLASS_ID_CUSTOM_PROPERTY_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.CLASS_ID_SEQUENCE_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.LOCAL_ENTITY_ID_PROPERTY_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.localEntityIdSequenceName
import java.util.concurrent.ConcurrentHashMap

interface OSchemaBuddy {
    fun initialize(session: ODatabaseDocumentInternal)

    fun getOEntityId(session: ODatabaseDocumentInternal, entityId: PersistentEntityId): ORIDEntityId

    /**
     * If the class has not been found, returns -1. It is how it was in the Classic Xodus.
     */
    fun getTypeId(session: ODatabaseDocumentInternal, entityType: String): Int

    fun requireTypeExists(session: ODatabaseDocumentInternal, entityType: String)

    fun getOrCreateSequence(session: ODatabaseDocumentInternal, sequenceName: String, initialValue: Long): OSequence

    fun getSequence(session: ODatabaseDocumentInternal, sequenceName: String): OSequence

    fun getSequenceOrNull(session: ODatabaseDocumentInternal, sequenceName: String): OSequence?

    fun updateSequence(session: ODatabaseDocumentInternal, sequenceName: String, currentValue: Long)

    fun renameOClass(session: ODatabaseDocumentInternal, oldName: String, newName: String)

    fun getOrCreateEdgeClass(session: ODatabaseDocumentInternal, linkName: String, outClassName: String, inClassName: String): OClass
}

class OSchemaBuddyImpl(
    private val dbProvider: ODatabaseProvider,
    autoInitialize: Boolean = true,
): OSchemaBuddy {
    companion object {
        val INTERNAL_CLASS_NAMES = hashSetOf(OClass.VERTEX_CLASS_NAME)
    }

    private val classIdToOClassId = ConcurrentHashMap<Int, Int>()

    init {
        if (autoInitialize) {
            dbProvider.withCurrentOrNewSession { session ->
                initialize(session)
            }
        }
    }

    override fun initialize(session: ODatabaseDocumentInternal) {
        session.createClassIdSequenceIfAbsent()
        for (oClass in session.metadata.schema.classes) {
            if (oClass.isVertexType && !INTERNAL_CLASS_NAMES.contains(oClass.name)) {
                classIdToOClassId[oClass.requireClassId()] = oClass.defaultClusterId
            }
        }
    }

    override fun getOrCreateSequence(session: ODatabaseDocumentInternal, sequenceName: String, initialValue: Long): OSequence {
        val oSequence = session.metadata.sequenceLibrary.getSequence(sequenceName)
        if (oSequence != null) return oSequence

        return session.executeInASeparateSessionIfCurrentHasTransaction(dbProvider) { sessionToWork ->
            val params = CreateParams().setStart(initialValue).setIncrement(1)
            sessionToWork.metadata.sequenceLibrary.createSequence(sequenceName, SEQUENCE_TYPE.ORDERED, params)
        }
    }

    override fun renameOClass(session: ODatabaseDocumentInternal, oldName: String, newName: String) {
        session.executeInASeparateSessionIfCurrentHasTransaction(dbProvider) { sessionToWork ->
            val oldClass = sessionToWork.metadata.schema.getClass(oldName) ?: throw IllegalArgumentException("Class $oldName not found")
            oldClass.setName(newName)
        }
    }

    override fun getOrCreateEdgeClass(session: ODatabaseDocumentInternal, linkName: String, outClassName: String, inClassName: String): OClass {
        val edgeClassName = OVertexEntity.edgeClassName(linkName)
        val oClass = session.getClass(edgeClassName)
        if (oClass != null) return oClass

        return session.executeInASeparateSessionIfCurrentHasTransaction(dbProvider) { sessionToWork ->
            sessionToWork.createEdgeClass(edgeClassName)
        }
    }

    override fun getSequence(session: ODatabaseDocumentInternal, sequenceName: String): OSequence {
        return session.metadata.sequenceLibrary.getSequence(sequenceName) ?: throw IllegalStateException("$sequenceName sequence not found")
    }

    override fun getSequenceOrNull(session: ODatabaseDocumentInternal, sequenceName: String): OSequence? {
        return session.metadata.sequenceLibrary.getSequence(sequenceName)
    }

    override fun updateSequence(session: ODatabaseDocumentInternal, sequenceName: String, currentValue: Long) {
        session.executeInASeparateSessionIfCurrentHasTransaction(dbProvider) { sessionToWork ->
            getSequence(sessionToWork, sequenceName).updateParams(CreateParams().setCurrentValue(currentValue))
        }
    }

    override fun getOEntityId(session: ODatabaseDocumentInternal, entityId: PersistentEntityId): ORIDEntityId {
        // Keep in mind that it is possible that we are given an entityId that is not in the database.
        // It is a valid case.

        val classId = entityId.typeId
        val localEntityId = entityId.localId
        val oClassId = classIdToOClassId[classId] ?: return ORIDEntityId.EMPTY_ID
        val className = session.getClusterNameById(oClassId) ?: return ORIDEntityId.EMPTY_ID
        val oClass = session.getClass(className) ?: return ORIDEntityId.EMPTY_ID

        val resultSet: OResultSet = session.query("SELECT FROM $className WHERE $LOCAL_ENTITY_ID_PROPERTY_NAME = ?", localEntityId)
        val oid = if (resultSet.hasNext()) {
            val result = resultSet.next()
            result.toVertex()?.identity ?: return ORIDEntityId.EMPTY_ID
        } else {
            return ORIDEntityId.EMPTY_ID
        }

        return ORIDEntityId(classId, localEntityId, oid, oClass)
    }

    override fun getTypeId(session: ODatabaseDocumentInternal, entityType: String): Int {
        return session.getClass(entityType)?.requireClassId() ?: -1
    }

    override fun requireTypeExists(session: ODatabaseDocumentInternal, entityType: String) {
        val oClass = session.getClass(entityType)
        check(oClass != null) { "$entityType has not been found" }
    }

}

fun <T> ODatabaseDocumentInternal.executeInASeparateSessionIfCurrentHasTransaction(dbProvider: ODatabaseProvider, action: (ODatabaseDocumentInternal) -> T): T {
    return if (this.hasActiveTransaction()) {
        dbProvider.executeInASeparateSession(this) { newSession ->
            action(newSession)
        }
    } else {
        action(this)
    }
}

fun ODatabaseDocumentInternal.createClassIdSequenceIfAbsent(startFrom: Long = -1L) {
    createSequenceIfAbsent(CLASS_ID_SEQUENCE_NAME, startFrom)
}

fun ODatabaseDocumentInternal.createLocalEntityIdSequenceIfAbsent(oClass: OClass, startFrom: Long = -1L) {
    createSequenceIfAbsent(localEntityIdSequenceName(oClass.name), startFrom)
}

private fun ODatabaseSession.createSequenceIfAbsent(sequenceName: String, startFrom: Long = 0L) {
    val sequences = metadata.sequenceLibrary
    if (sequences.getSequence(sequenceName) == null) {
        val params = CreateParams()
        params.start = startFrom
        sequences.createSequence(sequenceName, SEQUENCE_TYPE.ORDERED, params)
    }
}

fun ODatabaseSession.setClassIdIfAbsent(oClass: OClass) {
    if (oClass.getCustom(CLASS_ID_CUSTOM_PROPERTY_NAME) == null) {
        val sequences = metadata.sequenceLibrary
        val sequence: OSequence = sequences.getSequence(CLASS_ID_SEQUENCE_NAME) ?: throw IllegalStateException("$CLASS_ID_SEQUENCE_NAME not found")

        oClass.setCustom(CLASS_ID_CUSTOM_PROPERTY_NAME, sequence.next().toString())
    }
}

fun ODatabaseDocumentInternal.setLocalEntityId(className: String, vertex: OVertex) {
    val sequences = metadata.sequenceLibrary
    val sequenceName = localEntityIdSequenceName(className)
    val sequence: OSequence = sequences.getSequence(sequenceName) ?: throw IllegalStateException("$sequenceName not found")
    vertex.setProperty(LOCAL_ENTITY_ID_PROPERTY_NAME, sequence.next())
}

fun ODatabaseDocumentInternal.createVertexClassWithClassId(className: String): OClass {
    requireNoActiveTransaction()
    createClassIdSequenceIfAbsent()
    val oClass = createVertexClass(className)
    setClassIdIfAbsent(oClass)
    createLocalEntityIdSequenceIfAbsent(oClass)
    return oClass
}

internal fun ODatabaseDocumentInternal.getOrCreateVertexClass(className: String): OClass {
    val existingClass = this.getClass(className)
    if (existingClass != null) return existingClass

    return createVertexClassWithClassId(className)
}
