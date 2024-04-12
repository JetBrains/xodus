package jetbrains.exodus.query.metadata

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.record.impl.OVertexDocument
import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.XodusTestDB
import jetbrains.exodus.entitystore.orientdb.OVertexEntity
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.CLASS_ID_CUSTOM_PROPERTY_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.CLASS_ID_SEQUENCE_NAME
import jetbrains.exodus.entitystore.orientdb.testutil.InMemoryOrientDB
import org.junit.Assert
import org.junit.Rule
import org.junit.Test
import java.io.ByteArrayInputStream
import kotlin.test.assertEquals
import kotlin.test.assertTrue

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

        migrateDataFromXodusToOrientDb(xodus.store, orientDb.store)

        orientDb.withSession { oSession ->
            oSession.assertOrientContainsAllTheEntities(entities)
        }
    }

    @Test
    fun `copy blobs`() {
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

        migrateDataFromXodusToOrientDb(xodus.store, orientDb.store)

        orientDb.withSession { oSession ->
            oSession.assertOrientContainsAllTheEntities(entities)
        }
    }

    @Test
    fun `copy links`() {
        val entities = pileOfEntities(
            eLinks("type1", 1,
                Link("link1", "type1", 2), Link("link1", "type1", 3), // several links with the same name
                Link("link2", "type2", 4)
            ),
            eLinks("type1", 2,
                Link("link1", "type1", 1), // cycle
                Link("link2", "type2", 4)
            ),
            eLinks("type1", 3,
                Link("link1", "type1", 2), // cycle too
                Link("link2", "type2", 5)
            ),

            eLinks("type2", 4,
                Link("link2", "type2", 5)
            ),
            eLinks("type2", 5,
                Link("link1", "type1", 2), // cycle too
            ),
        )

        xodus.withTx { tx ->
            tx.createEntities(entities)
        }

        migrateDataFromXodusToOrientDb(xodus.store, orientDb.store)

        orientDb.withSession { oSession ->
            oSession.assertOrientContainsAllTheEntities(entities)
        }
    }

    @Test
    fun `if backward compatible EntityId enabled, copy existing class IDs and create the sequence to generate new class IDs`() {
        val entities = pileOfEntities(
            eProps("type1", 1),
            eProps("type1", 2),
            eProps("type1", 3),

            eProps("type2", 2),
            eProps("type2", 4),
            eProps("type2", 5),
        )
        xodus.withTx { tx ->
            tx.createEntities(entities)
        }

        migrateDataFromXodusToOrientDb(xodus.store, orientDb.store, backwardCompatibleEntityId = true)

        var maxClassId = 0
        xodus.withTx { xTx ->
            orientDb.withSession { oSession ->
                for (type in xTx.entityTypes) {
                    val typeId = xodus.store.getEntityTypeId(type)
                    Assert.assertEquals(typeId, oSession.getClass(type).getCustom(CLASS_ID_CUSTOM_PROPERTY_NAME).toInt())
                    maxClassId = maxOf(maxClassId, typeId)
                }
                assertTrue(maxClassId > 0)

                val nextGeneratedClassId = oSession.metadata.sequenceLibrary.getSequence(CLASS_ID_SEQUENCE_NAME).next()
                assertEquals(maxClassId.toLong() + 1, nextGeneratedClassId)
            }
        }
    }

    @Test
    fun `if backward compatible EntityId disabled, ignore class IDs`() {
        val entities = pileOfEntities(
            eProps("type1", 1),
            eProps("type1", 2),
            eProps("type1", 3),

            eProps("type2", 2),
            eProps("type2", 4),
            eProps("type2", 5),
        )
        xodus.withTx { tx ->
            tx.createEntities(entities)
        }

        migrateDataFromXodusToOrientDb(xodus.store, orientDb.store)

        xodus.withTx { xTx ->
            orientDb.withSession { oSession ->
                for (type in xTx.entityTypes) {
                    Assert.assertNull(oSession.getClass(type).getCustom(CLASS_ID_CUSTOM_PROPERTY_NAME))
                }
                Assert.assertNull(oSession.metadata.sequenceLibrary.getSequence(CLASS_ID_SEQUENCE_NAME))
            }
        }
    }

    private fun ODatabaseSession.assertOrientContainsAllTheEntities(pile: PileOfEntities) {
        for (type in pile.types) {
            for (record in this.browseClass(type)) {
                val entity = pile.getEntity(type, record.getId())
                record.assertEquals(entity)
            }
        }
    }

    private fun ODocument.assertEquals(expected: Entity) {
        val actualDocument = this
        val actual = OVertexEntity(actualDocument as OVertexDocument, orientDb.store)

        Assert.assertEquals(expected.id, actualDocument.getId())
        for ((propName, propValue) in expected.props) {
            Assert.assertEquals(propValue, actual.getProperty(propName))
        }
        for ((blobName, blobValue) in expected.blobs) {
            val actualValue = actual.getBlob(blobName)!!.readAllBytes()
            Assert.assertEquals(blobValue, actualValue.decodeToString())
        }

        for (expectedLink in expected.links) {
            val actualLinks = actual.getLinks(expectedLink.name).toList()
            val tartedActual = actualLinks.first { it.getProperty("id") == expectedLink.targetId }
            Assert.assertEquals(expectedLink.targetType, tartedActual.type)
        }
    }

    private fun ODocument.getId(): Int = getProperty("id")

    private fun StoreTransaction.createEntities(pile: PileOfEntities) {
        for (type in pile.types) {
            for (entity in pile.getAll(type)) {
                this.createEntity(entity)
            }
        }
        for (type in pile.types) {
            for (entity in pile.getAll(type)) {
                this.createLinks(entity)
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

    private fun StoreTransaction.createLinks(entity: Entity) {
        val xEntity = this.getAll(entity.type).first { it.getProperty("id") == entity.id }
        for (link in entity.links) {
            val targetXEntity = this.getAll(link.targetType).first { it.getProperty("id") == link.targetId }
            xEntity.addLink(link.name, targetXEntity)
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
    val blobs: Map<String, String>,
    val links: List<Link>
)

data class Link(
    val name: String,
    val targetType: String,
    val targetId : Int,
)

fun pileOfEntities(vararg entities: Entity): PileOfEntities {
    val pile = PileOfEntities()
    for (entity in entities) {
        pile.add(entity)
    }
    return pile
}

fun eProps(type: String, id: Int, vararg props: Pair<String, Comparable<*>>): Entity {
    return Entity(type, id, props.toMap(), mapOf(), listOf())
}

fun eBlobs(type: String, id: Int, vararg blobs: Pair<String, String>): Entity {
    return Entity(type, id, mapOf(), blobs.toMap(), listOf())
}

fun eLinks(type: String, id: Int, vararg links: Link): Entity {
    return Entity(type, id, mapOf(), mapOf(), links.toList())
}

