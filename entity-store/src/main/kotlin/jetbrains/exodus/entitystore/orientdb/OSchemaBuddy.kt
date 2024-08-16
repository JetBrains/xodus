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
    fun initialize(session: ODatabaseSession)

    fun getOEntityId(session: ODatabaseSession, entityId: PersistentEntityId): ORIDEntityId

    fun makeSureTypeExists(session: ODatabaseSession, entityType: String)

    fun getOrCreateSequence(session: ODatabaseSession, sequenceName: String, initialValue: Long): OSequence

    fun getSequence(session: ODatabaseSession, sequenceName: String): OSequence

    fun getSequenceOrNull(session: ODatabaseSession, sequenceName: String): OSequence?

    fun updateSequence(session: ODatabaseSession, sequenceName: String, currentValue: Long)

    fun renameOClass(session: ODatabaseSession, oldName: String, newName: String)
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

    override fun initialize(session: ODatabaseSession) {
        session.createClassIdSequenceIfAbsent()
        for (oClass in session.metadata.schema.classes) {
            if (oClass.isVertexType && !INTERNAL_CLASS_NAMES.contains(oClass.name)) {
                classIdToOClassId[oClass.requireClassId()] = oClass.defaultClusterId
            }
        }
    }

    override fun getOrCreateSequence(session: ODatabaseSession, sequenceName: String, initialValue: Long): OSequence {
        val oSequence = session.metadata.sequenceLibrary.getSequence(sequenceName)
        if (oSequence != null) return oSequence

        return executeInASeparateSessionIfCurrentHasTransaction(session) { sessionToWork ->
            val params = CreateParams().setStart(initialValue).setIncrement(1)
            sessionToWork.metadata.sequenceLibrary.createSequence(sequenceName, SEQUENCE_TYPE.ORDERED, params)
        }
    }

    override fun renameOClass(session: ODatabaseSession, oldName: String, newName: String) {
        executeInASeparateSessionIfCurrentHasTransaction(session) { sessionToWork ->
            val oldClass = sessionToWork.metadata.schema.getClass(oldName) ?: throw IllegalArgumentException("Class $oldName not found")
            oldClass.setName(newName)
        }
    }

    private inline fun <T> executeInASeparateSessionIfCurrentHasTransaction(session: ODatabaseSession, action: (ODatabaseSession) -> T): T {
        return if (session.hasActiveTransaction()) {
            dbProvider.executeInASeparateSession(session) { newSession ->
                action(newSession)
            }
        } else {
            action(session)
        }
    }

    override fun getSequence(session: ODatabaseSession, sequenceName: String): OSequence {
        return session.metadata.sequenceLibrary.getSequence(sequenceName) ?: throw IllegalStateException("$sequenceName sequence not found")
    }

    override fun getSequenceOrNull(session: ODatabaseSession, sequenceName: String): OSequence? {
        return session.metadata.sequenceLibrary.getSequence(sequenceName)
    }

    override fun updateSequence(session: ODatabaseSession, sequenceName: String, currentValue: Long) {
        executeInASeparateSessionIfCurrentHasTransaction(session) { sessionToWork ->
            getSequence(sessionToWork, sequenceName).updateParams(CreateParams().setCurrentValue(currentValue))
        }
    }

    override fun getOEntityId(session: ODatabaseSession, entityId: PersistentEntityId): ORIDEntityId {
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

    override fun makeSureTypeExists(session: ODatabaseSession, entityType: String) {
        val existingClass = session.getClass(entityType)
        if (existingClass != null) return

        val oClass = session.createVertexClassWithClassId(entityType)
        classIdToOClassId[oClass.requireClassId()] = oClass.defaultClusterId
    }

}

fun ODatabaseSession.createClassIdSequenceIfAbsent(startFrom: Long = -1L) {
    createSequenceIfAbsent(CLASS_ID_SEQUENCE_NAME, startFrom)
}

fun ODatabaseSession.createLocalEntityIdSequenceIfAbsent(oClass: OClass, startFrom: Long = -1L) {
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

internal fun ODatabaseSession.setLocalEntityIdIfAbsent(vertex: OVertex) {
    if (vertex.getProperty<Long>(LOCAL_ENTITY_ID_PROPERTY_NAME) == null) {
        val oClass = vertex.requireSchemaClass()
        setLocalEntityId(oClass.name, vertex)
    }
}

fun ODatabaseSession.setLocalEntityId(className: String, vertex: OVertex) {
    val sequences = metadata.sequenceLibrary
    val sequenceName = localEntityIdSequenceName(className)
    val sequence: OSequence = sequences.getSequence(sequenceName) ?: throw IllegalStateException("$sequenceName not found")
    vertex.setProperty(LOCAL_ENTITY_ID_PROPERTY_NAME, sequence.next())
}

fun ODatabaseSession.createVertexClassWithClassId(className: String): OClass {
    requireNoActiveTransaction()
    createClassIdSequenceIfAbsent()
    val oClass = createVertexClass(className)
    setClassIdIfAbsent(oClass)
    createLocalEntityIdSequenceIfAbsent(oClass)
    return oClass
}

internal fun ODatabaseSession.getOrCreateVertexClass(className: String): OClass {
    val existingClass = this.getClass(className)
    if (existingClass != null) return existingClass

    return createVertexClassWithClassId(className)
}