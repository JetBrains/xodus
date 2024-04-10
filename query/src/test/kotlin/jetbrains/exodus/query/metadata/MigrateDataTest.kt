package jetbrains.exodus.query.metadata

import com.orientechnologies.orient.core.record.impl.ODocument
import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.XodusTestDB
import jetbrains.exodus.entitystore.orientdb.testutil.InMemoryOrientDB
import org.junit.Assert
import org.junit.Rule
import org.junit.Test

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
            entity("type1", 1, "prop1" to "one", "prop2" to true, "prop3" to 1.1),
            entity("type1", 2, "prop1" to "two", "prop2" to true, "prop3" to 2.2),
            entity("type1", 3, "prop1" to "three", "prop2" to true, "prop3" to 3.3),

            entity("type2", 2, "pop1" to "four", "pop2" to true, "pop3" to 2.2),
            entity("type2", 4, "pop1" to "five", "pop2" to true, "pop3" to 3.3),
            entity("type2", 5, "pop1" to "six", "pop2" to true, "pop3" to 4.4),
        )
        xodus.withTx { tx ->
            for (type in entities.types) {
                for (entity in entities.getAll(type)) {
                    tx.createEntity(entity)
                }
            }
        }

        XodusToOrientDataMigrator(xodus.store, orientDb.store).migrate()

        orientDb.withSession { oSession ->
            for (type in entities.types) {
                for (record in oSession.browseClass(type)) {
                    val entity = entities.getEntity(type, record.getId())
                    assertEquals(entity, record)
                }
            }
        }
    }

    private fun assertEquals(expected: Entity, actual: ODocument) {
        Assert.assertEquals(expected.id, actual.getId())
        for ((propName, propValue) in expected.props) {
            Assert.assertEquals(propValue, actual.getProperty<Comparable<*>>(propName))
        }
    }

    private fun ODocument.getId(): Int = getProperty("id")

    private fun StoreTransaction.createEntity(entity: Entity) {
        val e = this.newEntity(entity.type)
        e.setProperty("id", entity.id)
        for ((name, value) in entity.props) {
            e.setProperty(name, value)
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
    val props: Map<String, Comparable<*>>
)

fun pileOfEntities(vararg entities: Entity): PileOfEntities {
    val pile = PileOfEntities()
    for (entity in entities) {
        pile.add(entity)
    }
    return pile
}

fun entity(type: String, id: Int, vararg props: Pair<String, Comparable<*>>): Entity {
    return Entity(type, id, props.toMap())
}