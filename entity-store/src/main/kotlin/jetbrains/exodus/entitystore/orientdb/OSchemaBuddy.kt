package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.metadata.sequence.OSequence
import com.orientechnologies.orient.core.record.OVertex
import com.orientechnologies.orient.core.sql.executor.OResultSet
import jetbrains.exodus.entitystore.PersistentEntityId
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.CLASS_ID_CUSTOM_PROPERTY_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.CLASS_ID_SEQUENCE_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.LOCAL_ENTITY_ID_PROPERTY_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.localEntityIdSequenceName
import java.util.concurrent.ConcurrentHashMap

interface OSchemaBuddy {
    fun getOEntityId(entityId: PersistentEntityId): ORIDEntityId
    fun makeSureTypeExists(session: ODatabaseDocument, entityType: String)
}

class OSchemaBuddyImpl(
    private val dbProvider: ODatabaseProvider,
    autoInitialize: Boolean = true,
): OSchemaBuddy {

    private val classIdToOClassId = ConcurrentHashMap<Int, Int>()

    init {
        if (autoInitialize) {
            initialize()
        }
    }

    fun initialize() {
        dbProvider.withCurrentOrNewSession { session ->
            session.createClassIdSequenceIfAbsent()
            for (oClass in session.metadata.schema.classes) {
                if (oClass.isVertexType && oClass.name != OClass.VERTEX_CLASS_NAME) {
                    classIdToOClassId.put(oClass.requireClassId(), oClass.defaultClusterId)
                }
            }
        }
    }

    override fun getOEntityId(entityId: PersistentEntityId): ORIDEntityId {
        // Keep in mind that it is possible that we are given an entityId that is not in the database.
        // It is a valid case.

        val oSession = ODatabaseSession.getActiveSession() ?: throw IllegalStateException("no active database session found")
        val classId = entityId.typeId
        val localEntityId = entityId.localId
        val oClassId = classIdToOClassId[classId] ?: return ORIDEntityId.EMPTY_ID
        val className = oSession.getClusterNameById(oClassId) ?: return ORIDEntityId.EMPTY_ID
        val oClass = oSession.getClass(className) ?: return ORIDEntityId.EMPTY_ID

        val resultSet: OResultSet = oSession.query("SELECT FROM $className WHERE $LOCAL_ENTITY_ID_PROPERTY_NAME = ?", localEntityId)
        val oid = if (resultSet.hasNext()) {
            val result = resultSet.next()
            result.toVertex()?.identity ?: return ORIDEntityId.EMPTY_ID
        } else {
            return ORIDEntityId.EMPTY_ID
        }

        return ORIDEntityId(classId, localEntityId, oid, oClass)
    }

    override fun makeSureTypeExists(session: ODatabaseDocument, entityType: String) {
        val existingClass = session.getClass(entityType)
        if (existingClass != null) return

        val oClass = session.createVertexClassWithClassId(entityType)
        classIdToOClassId[oClass.requireClassId()] = oClass.defaultClusterId
    }

}

fun ODatabaseDocument.createClassIdSequenceIfAbsent(startFrom: Long = 0L) {
    createSequenceIfAbsent(CLASS_ID_SEQUENCE_NAME, startFrom)
}

fun ODatabaseDocument.createLocalEntityIdSequenceIfAbsent(oClass: OClass, startFrom: Long = 0L) {
    createSequenceIfAbsent(localEntityIdSequenceName(oClass.name), startFrom)
}

fun ODatabaseDocument.createSequenceIfAbsent(sequenceName: String, startFrom: Long = 0L) {
    val sequences = metadata.sequenceLibrary
    if (sequences.getSequence(sequenceName) == null) {
        val params = OSequence.CreateParams()
        params.start = startFrom
        sequences.createSequence(sequenceName, OSequence.SEQUENCE_TYPE.ORDERED, params)
    }
}

fun ODatabaseDocument.setClassIdIfAbsent(oClass: OClass) {
    if (oClass.getCustom(CLASS_ID_CUSTOM_PROPERTY_NAME) == null) {
        val sequences = metadata.sequenceLibrary
        val sequence: OSequence = sequences.getSequence(CLASS_ID_SEQUENCE_NAME) ?: throw IllegalStateException("$CLASS_ID_SEQUENCE_NAME not found")

        oClass.setCustom(CLASS_ID_CUSTOM_PROPERTY_NAME, sequence.next().toString())
    }
}

fun ODatabaseDocument.setLocalEntityIdIfAbsent(vertex: OVertex) {
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

fun ODatabaseDocument.createVertexClassWithClassId(className: String): OClass {
    requireNoActiveTransaction()
    createClassIdSequenceIfAbsent()
    val oClass = createVertexClass(className)
    setClassIdIfAbsent(oClass)
    createLocalEntityIdSequenceIfAbsent(oClass)
    return oClass
}

fun ODatabaseDocument.getOrCreateVertexClass(className: String): OClass {
    val existingClass = this.getClass(className)
    if (existingClass != null) return existingClass

    return createVertexClassWithClassId(className)
}