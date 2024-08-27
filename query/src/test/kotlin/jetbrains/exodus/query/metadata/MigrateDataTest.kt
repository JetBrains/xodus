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
package jetbrains.exodus.query.metadata

import com.orientechnologies.orient.core.record.impl.OVertexDocument
import jetbrains.exodus.bindings.BindingUtils
import jetbrains.exodus.entitystore.PersistentEntityStore
import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.XodusTestDB
import jetbrains.exodus.entitystore.orientdb.OVertexEntity
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.CLASS_ID_SEQUENCE_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.localEntityIdSequenceName
import jetbrains.exodus.entitystore.orientdb.requireClassId
import jetbrains.exodus.entitystore.orientdb.requireLocalEntityId
import jetbrains.exodus.entitystore.orientdb.testutil.InMemoryOrientDB
import jetbrains.exodus.util.ByteArraySizedInputStream
import org.junit.Assert
import org.junit.Assert.assertNotEquals
import org.junit.Rule
import org.junit.Test
import java.io.ByteArrayInputStream
import java.nio.charset.MalformedInputException
import kotlin.test.*

class MigrateDataTest {

    @Rule
    @JvmField
    val orientDb = InMemoryOrientDB(initializeIssueSchema = false, autoInitializeSchemaBuddy = false)

    @Rule
    @JvmField
    val xodus = XodusTestDB()

    @OptIn(ExperimentalStdlibApi::class)
    @Test
    fun `broken strings from Xodus`() {
        // it is how the string is stored in Classic Xodus on the disk
        // to be absolutely precise you should add a single byte (82) to the beginning (the type identifier)
        val originalBytes = "e197b0cf83c4a7e2b1a2ceb1c59fc4a7ceb96d20e1b9a8c4a7ceb1e1b8adeda080c4a700".hexToByteArray()
        // it is how Classic Xodus decodes strings
        val originalStr = BindingUtils.readString(ByteArraySizedInputStream(originalBytes))

        // none of the charsets can decode the string to get original bytes
        val charsets = with(Charsets) {
            listOf(US_ASCII, ISO_8859_1, UTF_16, UTF_32, UTF_8, UTF_16BE, UTF_16LE, UTF_32BE, UTF_32LE)
        }
        for (charset in charsets) {
            val bytes = originalStr.toByteArray(charset)
            assertFalse(originalBytes.contentEquals(bytes))
        }

        // the original string is malformed
        assertFailsWith<MalformedInputException> { originalStr.encodeToByteArray(throwOnInvalidSequence = true) }

        // let's migrate such a string from Classic Xodus to Orient and see what happens
        val entities = pileOfEntities(
            eProps("type1", 1, "prop1" to originalStr)
        )
        xodus.withTx { tx -> tx.createEntities(entities) }
        migrateDataFromXodusToOrientDb(xodus.store, orientDb)

        xodus.withTx { xTx ->
            orientDb.withStoreTx { oTx ->
                val xStr = xTx.getAll("type1").first().getProperty("prop1") as String
                val oStr = oTx.getAll("type1").first().getProperty("prop1") as String

                // string in Xodus equals to the original string
                assertEquals(originalStr, xStr)
                // string in Orient does not equal to the original string
                assertNotEquals(xStr, oStr)

                // fix the string from Xodus
                val fixedXStr = xStr.encodeToByteArray(throwOnInvalidSequence = false).decodeToString(throwOnInvalidSequence = true)

                // now, fixed Xodus string equals to the Orient string
                assertEquals(fixedXStr, oStr)
                println("""
                    xStr:      '$xStr'
                    fixedXStr: '$fixedXStr'
                    oStr:      '$oStr'
                """.trimIndent())
            }
        }
    }

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

        migrateDataFromXodusToOrientDb(xodus.store, orientDb)

        orientDb.store.executeInTransaction { tx ->
            tx.assertOrientContainsAllTheEntities(entities)
        }
    }

    @Test
    fun `copy an empty entity`() {
        val entityId = xodus.withTx { tx ->
            tx.newEntity("type1").id
        }

        migrateDataFromXodusToOrientDb(xodus.store, orientDb)
        orientDb.withSession {
            orientDb.schemaBuddy.initialize(it)
        }

        orientDb.store.executeInTransaction {
            orientDb.store.getEntity(entityId)
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
            eStringBlobs("type2", 7, "alice" to "cooper"),
            eStringBlobs("type2", 8, "alice" to "coopercooper"),
        )

        xodus.withTx { tx ->
            tx.createEntities(entities)
        }

        migrateDataFromXodusToOrientDb(xodus.store, orientDb)

        orientDb.store.executeInTransaction { tx ->
            tx.assertOrientContainsAllTheEntities(entities)
        }
    }

    @Test
    fun `copy links`() {
        val entities = pileOfEntities(
            eLinks(
                "type1", 1,
                Link("link1", "type1", 2), Link("link1", "type1", 3), // several links with the same name
                Link("link2", "type2", 4)
            ),
            eLinks(
                "type1", 2,
                Link("link1", "type1", 1), // cycle
                Link("link2", "type2", 4)
            ),
            eLinks(
                "type1", 3,
                Link("link1", "type1", 2), // cycle too
                Link("link2", "type2", 5)
            ),

            eLinks(
                "type2", 4,
                Link("link2", "type2", 5)
            ),
            eLinks(
                "type2", 5,
                Link("link1", "type1", 2), // cycle too
            ),
        )

        xodus.withTx { tx ->
            tx.createEntities(entities)
        }

        migrateDataFromXodusToOrientDb(xodus.store, orientDb)

        orientDb.store.executeInTransaction { tx ->
            tx.assertOrientContainsAllTheEntities(entities)
        }
    }

    @Test
    fun `copy links with questionable names`() {
        val entities = pileOfEntities(
            eLinks(
                "type1", 1,
                Link("link2/cava/banga", "type2", 2)
            ),
            eLinks("type2", 2)
        )

        xodus.withTx { tx ->
            tx.createEntities(entities)
        }

        migrateDataFromXodusToOrientDb(xodus.store, orientDb)

        orientDb.store.executeInTransaction { tx ->
            tx.assertOrientContainsAllTheEntities(entities)
        }
    }

    @Test
    fun `copy existing class IDs and create the sequence to generate new class IDs`() {
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

        migrateDataFromXodusToOrientDb(xodus.store, orientDb)

        var maxClassId = 0
        xodus.withTx { xTx ->
            orientDb.withSession { oSession ->
                for (type in xTx.entityTypes) {
                    val typeId = xodus.store.getEntityTypeId(type)
                    Assert.assertEquals(typeId, oSession.getClass(type).requireClassId())
                    maxClassId = maxOf(maxClassId, typeId)
                }
                assertTrue(maxClassId > 0)

                val nextGeneratedClassId = oSession.metadata.sequenceLibrary.getSequence(CLASS_ID_SEQUENCE_NAME).next()
                assertEquals(maxClassId.toLong() + 1, nextGeneratedClassId)
            }
        }
    }

    @Test
    fun `copy localEntityId for every entity and create a sequence for every class to generate new localEntityIds`() {
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

        migrateDataFromXodusToOrientDb(xodus.store, orientDb)

        xodus.withTx { xTx ->
            orientDb.withSession { oSession ->
                for (type in xTx.entityTypes) {
                    val xTestIdToLocalEntityId = HashMap<Int, Long>()
                    val oTestIdToLocalEntityId = HashMap<Int, Long>()
                    var maxLocalEntityId = 0L
                    for (xEntity in xTx.getAll(type)) {
                        val testId = xEntity.getProperty("id") as Int
                        val localEntityId = xEntity.id.localId
                        xTestIdToLocalEntityId[testId] = localEntityId
                        maxLocalEntityId = maxOf(maxLocalEntityId, localEntityId)
                    }

                    for (oEntity in oSession.browseClass(type).map { it as OVertexDocument }) {
                        val testId = oEntity.getTestId()
                        val localEntityId = oEntity.requireLocalEntityId()
                        oTestIdToLocalEntityId[testId] = localEntityId
                    }

                    assertTrue(maxLocalEntityId > 0)
                    val nextGeneratedLocalEntityId =
                        oSession.metadata.sequenceLibrary.getSequence(localEntityIdSequenceName(type)).next()
                    assertEquals(maxLocalEntityId + 1, nextGeneratedLocalEntityId)

                    assertEquals(xTestIdToLocalEntityId, oTestIdToLocalEntityId)
                }
            }
        }
    }

}

internal fun migrateDataFromXodusToOrientDb(xodus: PersistentEntityStore, orient: InMemoryOrientDB) =
    migrateDataFromXodusToOrientDb(
        xodus,
        orient.store,
        orient.provider,
        orient.schemaBuddy
    )

internal fun StoreTransaction.assertOrientContainsAllTheEntities(pile: PileOfEntities) {
    for (type in pile.types) {
        for (record in this.getAll(type).map { it as OVertexEntity }) {
            val entity = pile.getEntity(type, record.getTestId())
            record.assertEquals(entity)
        }
    }
}

internal fun OVertexEntity.assertEquals(expected: Entity) {
    val actualDocument = this
    val actual = this

    Assert.assertEquals(expected.id, actualDocument.getTestId())
    for (propName in expected.props.keys) {
        val expectedValue = expected.props.getValue(propName)
        val actualValue = actual.getProperty(propName)
        Assert.assertEquals(expectedValue, actualValue)
    }
    for ((blobName, blobValue) in expected.blobs) {
        val actualValue = actual.getBlob(blobName)!!.readAllBytes()
        Assert.assertEquals(blobValue, actualValue.decodeToString())
    }
    for ((blobName, blobValue) in expected.stringBlobs) {
        val actualValue = actual.getBlobString(blobName)!!
        Assert.assertEquals(blobValue, actualValue)
    }

    for (expectedLink in expected.links) {
        val actualLinks = actual.getLinks(expectedLink.name).toList()
        val tartedActual = actualLinks.first { it.getProperty("id") == expectedLink.targetId }
        Assert.assertEquals(expectedLink.targetType, tartedActual.type)
    }
}

internal fun OVertexEntity.getTestId(): Int = getProperty("id") as Int

internal fun OVertexDocument.getTestId(): Int = getProperty("id") as Int

internal fun StoreTransaction.createEntities(pile: PileOfEntities) {
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

internal fun StoreTransaction.createEntity(entity: Entity) {
    val e = this.newEntity(entity.type)
    e.setProperty("id", entity.id)

    for ((name, value) in entity.props) {
        e.setProperty(name, value)
    }
    for ((name, value) in entity.blobs) {
        e.setBlob(name, ByteArrayInputStream(value.encodeToByteArray()))
    }
    for ((name, value) in entity.stringBlobs) {
        e.setBlobString(name, value)
    }
}

internal fun StoreTransaction.createLinks(entity: Entity) {
    val xEntity = this.getAll(entity.type).first { it.getProperty("id") == entity.id }
    for (link in entity.links) {
        val targetXEntity = this.getAll(link.targetType).first { it.getProperty("id") == link.targetId }
        xEntity.addLink(link.name, targetXEntity)
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
    val stringBlobs: Map<String, String>,
    val links: List<Link>
)

data class Link(
    val name: String,
    val targetType: String,
    val targetId: Int,
)

fun pileOfEntities(vararg entities: Entity): PileOfEntities {
    val pile = PileOfEntities()
    for (entity in entities) {
        pile.add(entity)
    }
    return pile
}

fun eProps(type: String, id: Int, vararg props: Pair<String, Comparable<*>>): Entity {
    return Entity(type, id, props.toMap(), mapOf(), mapOf(), listOf())
}

fun eBlobs(type: String, id: Int, vararg blobs: Pair<String, String>): Entity {
    return Entity(type, id, mapOf(), blobs.toMap(), mapOf(), listOf())
}

fun eStringBlobs(type: String, id: Int, vararg blobs: Pair<String, String>): Entity {
    return Entity(type, id, mapOf(), mapOf(), blobs.toMap(), listOf())
}

fun eLinks(type: String, id: Int, vararg links: Link): Entity {
    return Entity(type, id, mapOf(), mapOf(), mapOf(), links.toList())
}

