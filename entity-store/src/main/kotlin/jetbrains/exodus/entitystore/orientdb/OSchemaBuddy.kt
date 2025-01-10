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

import com.jetbrains.youtrack.db.api.DatabaseSession
import com.jetbrains.youtrack.db.api.query.ResultSet
import com.jetbrains.youtrack.db.api.record.Vertex
import com.jetbrains.youtrack.db.api.schema.SchemaClass
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.DBSequence
import jetbrains.exodus.entitystore.EntityRemovedInDatabaseException
import jetbrains.exodus.entitystore.PersistentEntityId
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.CLASS_ID_CUSTOM_PROPERTY_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.CLASS_ID_SEQUENCE_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.LOCAL_ENTITY_ID_PROPERTY_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.localEntityIdSequenceName
import java.util.concurrent.ConcurrentHashMap

interface OSchemaBuddy {
    fun initialize(session: DatabaseSession)

    fun getOEntityId(session: DatabaseSession, entityId: PersistentEntityId): ORIDEntityId

    /**
     * If the class has not been found, returns -1. It is how it was in the Classic Xodus.
     */
    fun getTypeId(session: DatabaseSession, entityType: String): Int

    fun getType(session: DatabaseSession, entityTypeId: Int): String

    fun requireTypeExists(session: DatabaseSession, entityType: String)

    fun getOrCreateSequence(
        session: DatabaseSession,
        sequenceName: String,
        initialValue: Long
    ): DBSequence

    fun getSequence(session: DatabaseSession, sequenceName: String): DBSequence

    fun getSequenceOrNull(session: DatabaseSession, sequenceName: String): DBSequence?

    fun updateSequence(session: DatabaseSession, sequenceName: String, currentValue: Long)

    fun renameOClass(session: DatabaseSession, oldName: String, newName: String)

    fun getOrCreateEdgeClass(
        session: DatabaseSession,
        linkName: String,
        outClassName: String,
        inClassName: String
    ): SchemaClass
}

class OSchemaBuddyImpl(
    private val dbProvider: ODatabaseProvider,
    autoInitialize: Boolean = true,
) : OSchemaBuddy {
    companion object {
        val INTERNAL_CLASS_NAMES = hashSetOf(SchemaClass.VERTEX_CLASS_NAME)
    }

    private val classIdToOClassId = ConcurrentHashMap<Int, Pair<Int, String>>()

    init {
        if (autoInitialize) {
            dbProvider.withCurrentOrNewSession { session ->
                initialize(session)
            }
        }
    }

    override fun initialize(session: DatabaseSession) {
        session.createClassIdSequenceIfAbsent()
        for (oClass in session.schema.classes) {
            if (oClass.isVertexType && !INTERNAL_CLASS_NAMES.contains(oClass.name)) {
                classIdToOClassId[oClass.requireClassId()] = oClass.clusterIds[0] to oClass.name
            }
        }
    }

    override fun getOrCreateSequence(
        session: DatabaseSession,
        sequenceName: String,
        initialValue: Long
    ): DBSequence {
        val oSequence =
            (session as DatabaseSessionInternal).metadata.sequenceLibrary.getSequence(sequenceName)
        if (oSequence != null) return oSequence

        return session.executeInASeparateSessionIfCurrentHasTransaction(dbProvider) { sessionToWork ->
            val params = DBSequence.CreateParams().setStart(initialValue).setIncrement(1)
            (sessionToWork as DatabaseSessionInternal).metadata.sequenceLibrary.createSequence(
                sequenceName,
                DBSequence.SEQUENCE_TYPE.ORDERED,
                params
            )
        }
    }

    override fun renameOClass(session: DatabaseSession, oldName: String, newName: String) {
        session.executeInASeparateSessionIfCurrentHasTransaction(dbProvider) { sessionToWork ->
            val oldClass = sessionToWork.schema.getClass(oldName)
                ?: throw IllegalArgumentException("Class $oldName not found")
            oldClass.setName(sessionToWork, newName)
        }
    }

    override fun getOrCreateEdgeClass(
        session: DatabaseSession,
        linkName: String,
        outClassName: String,
        inClassName: String
    ): SchemaClass {
        val edgeClassName = OVertexEntity.edgeClassName(linkName)
        val oClass = session.getClass(edgeClassName)
        if (oClass != null) return oClass

        return session.executeInASeparateSessionIfCurrentHasTransaction(dbProvider) { sessionToWork ->
            sessionToWork.createEdgeClass(edgeClassName)
        }
    }

    override fun getSequence(session: DatabaseSession, sequenceName: String): DBSequence {
        return (session as DatabaseSessionInternal).metadata.sequenceLibrary.getSequence(
            sequenceName
        )
            ?: throw IllegalStateException("$sequenceName sequence not found")
    }

    override fun getSequenceOrNull(session: DatabaseSession, sequenceName: String): DBSequence? {
        return (session as DatabaseSessionInternal).metadata.sequenceLibrary.getSequence(
            sequenceName
        )
    }

    override fun updateSequence(
        session: DatabaseSession,
        sequenceName: String,
        currentValue: Long
    ) {
        session.executeInASeparateSessionIfCurrentHasTransaction(dbProvider) { sessionToWork ->
            sessionToWork.begin()
            getSequence(sessionToWork, sequenceName).updateParams(
                DBSequence.CreateParams().setCurrentValue(
                    currentValue
                )
            )
            sessionToWork.commit()
        }
    }

    override fun getOEntityId(
        session: DatabaseSession,
        entityId: PersistentEntityId
    ): ORIDEntityId {
        // Keep in mind that it is possible that we are given an entityId that is not in the database.
        // It is a valid case.

        val classId = entityId.typeId
        val localEntityId = entityId.localId
        val oClassId = classIdToOClassId[classId]?.first ?: return ORIDEntityId.EMPTY_ID
        val schema = session.schema
        val oClass = schema.getClassByClusterId(oClassId) ?: return ORIDEntityId.EMPTY_ID

        val resultSet: ResultSet = session.query(
            "SELECT FROM ${oClass.name} WHERE $LOCAL_ENTITY_ID_PROPERTY_NAME = ?",
            localEntityId
        )
        val oid = if (resultSet.hasNext()) {
            val result = resultSet.next()
            result.toVertex()?.identity ?: return ORIDEntityId.EMPTY_ID
        } else {
            return ORIDEntityId.EMPTY_ID
        }

        return ORIDEntityId(classId, localEntityId, oid, oClass)
    }

    override fun getTypeId(session: DatabaseSession, entityType: String): Int {
        return session.getClass(entityType)?.requireClassId() ?: -1
    }

    override fun getType(
        session: DatabaseSession,
        entityTypeId: Int
    ): String {
        val (_, typeName) = classIdToOClassId.computeIfAbsent(entityTypeId) {
            val oClass = session.schema.classes.find { oClass ->
                oClass.getCustom(CLASS_ID_CUSTOM_PROPERTY_NAME)?.toInt() == entityTypeId
            } ?: throw EntityRemovedInDatabaseException("Invalid type ID $entityTypeId")
            oClass.requireClassId() to oClass.name
        }
        return typeName
    }

    override fun requireTypeExists(session: DatabaseSession, entityType: String) {
        val oClass = session.getClass(entityType)
        check(oClass != null) { "$entityType has not been found" }
    }

}

fun <T> DatabaseSession.executeInASeparateSessionIfCurrentHasTransaction(
    dbProvider: ODatabaseProvider,
    action: (DatabaseSession) -> T
): T {
    return if (this.hasActiveTransaction()) {
        dbProvider.executeInASeparateSession(this) { newSession ->
            action(newSession)
        }
    } else {
        action(this)
    }
}

fun DatabaseSession.createClassIdSequenceIfAbsent(startFrom: Long = -1L) {
    createSequenceIfAbsent(CLASS_ID_SEQUENCE_NAME, startFrom)
}

fun DatabaseSession.createLocalEntityIdSequenceIfAbsent(
    oClass: SchemaClass,
    startFrom: Long = -1L
) {
    createSequenceIfAbsent(localEntityIdSequenceName(oClass.name), startFrom)
}

private fun DatabaseSession.createSequenceIfAbsent(sequenceName: String, startFrom: Long = 0L) {
    val sequences = (this as DatabaseSessionInternal).metadata.sequenceLibrary
    if (sequences.getSequence(sequenceName) == null) {
        val params = DBSequence.CreateParams()
        params.start = startFrom
        sequences.createSequence(sequenceName, DBSequence.SEQUENCE_TYPE.ORDERED, params)
    }
}

fun DatabaseSession.setClassIdIfAbsent(oClass: SchemaClass) {
    if (oClass.getCustom(CLASS_ID_CUSTOM_PROPERTY_NAME) == null) {
        val sequences = (this as DatabaseSessionInternal).metadata.sequenceLibrary
        val sequence: DBSequence = sequences.getSequence(CLASS_ID_SEQUENCE_NAME)
            ?: throw IllegalStateException("$CLASS_ID_SEQUENCE_NAME not found")

        oClass.setCustom(this, CLASS_ID_CUSTOM_PROPERTY_NAME, sequence.next().toString())
    }
}

fun DatabaseSession.setLocalEntityId(className: String, vertex: Vertex) {
    val sequences = (this as DatabaseSessionInternal).metadata.sequenceLibrary
    val sequenceName = localEntityIdSequenceName(className)
    val sequence: DBSequence = sequences.getSequence(sequenceName)
        ?: throw IllegalStateException("$sequenceName not found")
    vertex.setProperty(LOCAL_ENTITY_ID_PROPERTY_NAME, sequence.next())
}

fun DatabaseSession.createVertexClassWithClassId(className: String): SchemaClass {
    requireNoActiveTransaction()
    createClassIdSequenceIfAbsent()
    val oClass = createVertexClass(className)
    setClassIdIfAbsent(oClass)
    createLocalEntityIdSequenceIfAbsent(oClass)
    return oClass
}

internal fun DatabaseSession.getOrCreateVertexClass(className: String): SchemaClass {
    val existingClass = this.getClass(className)
    if (existingClass != null) return existingClass

    return createVertexClassWithClassId(className)
}
