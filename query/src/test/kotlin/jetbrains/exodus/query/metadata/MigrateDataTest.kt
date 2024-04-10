package jetbrains.exodus.query.metadata

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.record.impl.OVertexDocument
import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.XodusTestDB
import jetbrains.exodus.entitystore.orientdb.OVertexEntity
import jetbrains.exodus.entitystore.orientdb.testutil.InMemoryOrientDB
import org.junit.Assert
import org.junit.Rule
import org.junit.Test
import java.io.ByteArrayInputStream

class MigrateDataTest {

    @Rule
    @JvmField
    val orientDb = InMemoryOrientDB(createClasses = false)

    @Rule
    @JvmField
    val xodus = XodusTestDB()

    @Test
    fun `copy properties`() {
        val entities = pileOfEntities(
            eProps("type1", 1, "prop1" to "one", "prop2" to true, "prop3" to 1.1),
            eProps("type1", 2, "prop1" to "two", "prop2" to true, "prop3" to 2.2),
            eProps("type1", 3, "prop1" to "three", "prop2" to true, "prop3" to 3.3),

            eProps("type2", 2, "pop1" to "four", "pop2" to true, "pop3" to 2.2),
            eProps("type2", 4, "pop1" to "five", "pop2" to true, "pop3" to 3.3),
            eProps("type2", 5, "pop1" to "six", "pop2" to true, "pop3" to 4.4),
        )
        xodus.withTx { tx ->
            tx.createEntities(entities)
        }

        XodusToOrientDataMigrator(xodus.store, orientDb.store).migrate()

        orientDb.withSession { oSession ->
            oSession.assertOrientContainsAllTheEntities(entities)
        }
    }

    @Test
    fun `copy blobs`() {
        ByteArrayInputStream("".encodeToByteArray())
        val entities = pileOfEntities(
            eBlobs("type1", 1, "blob1" to "one"),
            eBlobs("type1", 2, "blob1" to "two"),
            eBlobs("type1", 3, "blob1" to "three"),

            eBlobs("type2", 4, "bob1" to "four"),
            eBlobs("type2", 5, "bob1" to "five"),
            eBlobs("type2", 6, "bob1" to "six"),
        )

        xodus.withTx { tx ->
            tx.createEntities(entities)
        }

        XodusToOrientDataMigrator(xodus.store, orientDb.store).migrate()

        orientDb.withSession { oSession ->
            oSession.assertOrientContainsAllTheEntities(entities)
        }
    }

    private fun ODatabaseSession.assertOrientContainsAllTheEntities(pile: PileOfEntities) {
        for (type in pile.types) {
            for (record in this.browseClass(type)) {
                val entity = pile.getEntity(type, record.getId())
                assertEquals(entity, record)
            }
        }
    }

    private fun assertEquals(expected: Entity, actualDocument: ODocument) {
        val actual = OVertexEntity(actualDocument as OVertexDocument, orientDb.store)

        Assert.assertEquals(expected.id, actualDocument.getId())
        for ((propName, propValue) in expected.props) {
            Assert.assertEquals(propValue, actual.getProperty(propName))
        }
        for ((blobName, blobValue) in expected.blobs) {
            val actualValue = actual.getBlob(blobName)!!.readAllBytes()
            Assert.assertEquals(blobValue, actualValue.decodeToString())
        }
    }

    private fun ODocument.getId(): Int = getProperty("id")

    private fun StoreTransaction.createEntities(pile: PileOfEntities) {
        for (type in pile.types) {
            for (entity in pile.getAll(type)) {
                this.createEntity(entity)
            }
        }
    }

    private fun StoreTransaction.createEntity(entity: Entity) {
        val e = this.newEntity(entity.type)
        e.setProperty("id", entity.id)

        for ((name, value) in entity.props) {
            e.setProperty(name, value)
        }
        for ((name, value) in entity.blobs) {
            e.setBlob(name, ByteArrayInputStream(value.encodeToByteArray()))
        }
    }
}

class PileOfEntities {
    private val typeToEntities = mutableMapOf<String, MutableMap<Int, Entity>>()

    val types: Set<String> get() = typeToEntities.keys

    fun add(entity: Entity) {
        typeToEntities.getOrPut(entity.type) { mutableMapOf() }[entity.id] = entity
    }

    fun getAll(type: String): Collection<Entity> = typeToEntities.getValue(type).values

    fun getEntity(type: String, id: Int): Entity = typeToEntities.getValue(type).getValue(id)
}

data class Entity(
    val type: String,
    val id: Int,
    val props: Map<String, Comparable<*>>,
    val blobs: Map<String, String>
)

fun pileOfEntities(vararg entities: Entity): PileOfEntities {
    val pile = PileOfEntities()
    for (entity in entities) {
        pile.add(entity)
    }
    return pile
}

fun eProps(type: String, id: Int, vararg props: Pair<String, Comparable<*>>): Entity {
    return Entity(type, id, props.toMap(), mapOf())
}

fun eBlobs(type: String, id: Int, vararg blobs: Pair<String, String>): Entity {
    return Entity(type, id, mapOf(), blobs.toMap())
}

