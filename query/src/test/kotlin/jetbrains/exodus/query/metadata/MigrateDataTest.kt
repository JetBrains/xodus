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
@file:OptIn(ExperimentalStdlibApi::class)

package jetbrains.exodus.query.metadata

import com.orientechnologies.orient.core.record.OVertex
import com.orientechnologies.orient.core.record.impl.ORecordBytes
import com.orientechnologies.orient.core.record.impl.OVertexDocument
import jetbrains.exodus.bindings.ComparableSet
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.entitystore.PersistentEntityStore
import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.XodusTestDB
import jetbrains.exodus.entitystore.orientdb.OVertexEntity
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.CLASS_ID_SEQUENCE_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.localEntityIdSequenceName
import jetbrains.exodus.entitystore.orientdb.createVertexClassWithClassId
import jetbrains.exodus.entitystore.orientdb.requireClassId
import jetbrains.exodus.entitystore.orientdb.requireLocalEntityId
import jetbrains.exodus.entitystore.orientdb.testutil.InMemoryOrientDB
import jetbrains.exodus.util.ByteArraySizedInputStream
import jetbrains.exodus.util.LightOutputStream
import jetbrains.exodus.util.UTFUtil
import org.junit.Assert
import org.junit.Assert.assertNotEquals
import org.junit.Rule
import org.junit.Test
import java.io.ByteArrayInputStream
import java.nio.charset.MalformedInputException
import kotlin.random.Random
import kotlin.test.*

class MigrateDataTest {

    @Rule
    @JvmField
    val orientDb = InMemoryOrientDB(initializeIssueSchema = false, autoInitializeSchemaBuddy = false)

    @Rule
    @JvmField
    val xodus = XodusTestDB()

    @Test
    @Ignore
    fun `Xodus home UTF-8 is broken, example`() {
        repeat(100_000) { i ->
            val str = randomUtf8String(10)

            str.encodeToByteArray(throwOnInvalidSequence = true)
            val bytesXodusStyle = str.toBytesXodusPropertyStyle(true)
            val bytes = str.toByteArray(Charsets.UTF_8)
            if (!bytes.contentEquals(bytesXodusStyle)) {
                val strXodusStyle = bytesXodusStyle.toStringXodusPropertyStyle(addEOF = true)
                val strNormal = bytes.toString(Charsets.UTF_8)
                val hexXodusStyle = bytesXodusStyle.toHexString()
                val hex = bytes.toHexString()
                println("""
                    $i problematic string
                    original: '$str'
                    normal  : '$strNormal'
                    xStyle  : '$strXodusStyle'
                    bytes   : '$hex'
                    xBytes  : '$hexXodusStyle'
                """.trimIndent())
                throw Exception()
            }
        }
    }

    @Test
    fun `migrate string blobs`() {
        orientDb.withSession { session ->
            session.createVertexClassWithClassId("type1")
        }

        val str = """
            mamba, mamba, caramba
            меджик пипл, woodoo пипл
            Вы хотите песен, их есть у меня
        """.trimIndent()

        val xId = xodus.withTx { tx ->
            val e1 = tx.newEntity("type1")
            e1.setBlobString("blob1", str)
            e1.id
        }

        // it is how we migrate string blobs
        // we do not know which blobs are strings and which are not at the migration step,
        // so we copy all of them just as blobs
        val oId = xodus.withTx { xTx ->
            val xE = xTx.getEntity(xId)
            val xBlob = xE.getBlob("blob1")!!
            orientDb.withStoreTx { oTx ->
                val oE = oTx.newEntity("type1")
                oE.setBlob("blob1", xBlob)
                oE.id
            }
        }

        xodus.withTx { xTx ->
            val xE = xTx.getEntity(xId)
            val xBytes = xE.getBlob("blob1")!!.readAllBytes()
            orientDb.withStoreTx { tx ->
                val oE = tx.getEntity(oId)
                val oBytes = oE.getBlob("blob1")!!.readAllBytes()
                assertContentEquals(xBytes, oBytes)

                val oBlob = oE.getBlob("blob1")!!
                val oStr1 = UTFUtil.readUTF(oBlob)
                assertEquals(str, oStr1)

                val oStr2 = oE.getBlobString("blob1")!!
                assertEquals(str, oStr2)
            }
        }
    }

    /**
     * An illustration for a problem in Orient
     */
    @Test
    fun `orient add extra bytes to blobs once in a while`() {
        orientDb.withSession { session ->
            session.createVertexClass("turbo")
        }

        val brokenSizes = mutableSetOf<String>()
        for (size in 1..1500) {
            val bytes = Random.nextBytes(size)
            val message = StringBuilder()
            message.append("given $size bytes")

            val id = orientDb.withTxSession { session ->
                val e1 = session.newVertex("turbo")
                val blob = ORecordBytes()
                // this thing reads more bytes than necessary
                val readBytes = blob.fromInputStream(ByteArrayInputStream(bytes))
                message.append(", fromInputStream() read $readBytes bytes")
                e1.setProperty("blob1", blob)
                e1.save<OVertex>()
                e1.identity
            }

            orientDb.withTxSession { session ->
                val e1 = session.getRecord<OVertex>(id)
                val blob = e1.getProperty<ORecordBytes>("blob1")
                val gotBytes = blob.toStream()
                message.append(", got ${gotBytes.size} from the database")
                if (!gotBytes.contentEquals(bytes)) {
                    brokenSizes.add(message.toString())
                }
            }
        }
        println(brokenSizes.joinToString("\n"))
    }

    /**
     * Xodus encodes/decodes strings for properties in its own way.
     * It is supposed to be UTF-8, but something went wrong.
     *
     * This fact brought us a bunch of problems from the migration to Orient point of view.
     *
     * So, this test is an illustration of the situation.
     */
    @Test
    fun `broken strings from Xodus`() {
        // it is how the string is stored in Classic Xodus on the disk
        // The first byte is the type identifier (82), the last byte is the end of string (00)
        val rawBytesGottenFromAProdXodus = "82e197b0cf83c4a7e2b1a2ceb1c59fc4a7ceb96d20e1b9a8c4a7ceb1e1b8adeda080c4a700".hexToByteArray()
        // drop the type identifier for the Xodus decoder
        val originalBytesForXodus = rawBytesGottenFromAProdXodus.sliceArray(1 until rawBytesGottenFromAProdXodus.size)
        // drop the type identifier and the end of string symbol for "normal" decoders
        val originalBytes = rawBytesGottenFromAProdXodus.sliceArray(1 until rawBytesGottenFromAProdXodus.size - 1)

        // Xodus does not parse a string with the type identifier
        assertFailsWith<IllegalArgumentException> {
            rawBytesGottenFromAProdXodus.toStringXodusPropertyStyle()
        }
        // Xodus does not parse a string without the end of file symbol
        assertFailsWith<ArrayIndexOutOfBoundsException> {
            originalBytes.toStringXodusPropertyStyle()
        }

        // it is how Classic Xodus decodes strings
        val originalStrXodusPropStyle = originalBytesForXodus.toStringXodusPropertyStyle()

        val charsets = with(Charsets) {
            listOf(US_ASCII, ISO_8859_1, UTF_16, UTF_32, UTF_8, UTF_16BE, UTF_16LE, UTF_32BE, UTF_32LE)
        }
        // none of the charsets can encode the string to get original bytes
        for (charset in charsets) {
            val bytes = originalStrXodusPropStyle.toByteArray(charset)
            assertFalse(originalBytes.contentEquals(bytes))
            assertFalse(originalBytesForXodus.contentEquals(bytes))
        }
        // none of the charsets can decode the original bytes to get the same string as Xodus does
        for (charset in charsets) {
            val str1 = originalBytes.toString(charset)
            val str2 = originalBytesForXodus.toString(charset)
            assertNotEquals(originalStrXodusPropStyle, str1)
            assertNotEquals(originalStrXodusPropStyle, str2)
        }

        // the original Xodus-property-style string is malformed from the UTF-8 point of view
        assertFailsWith<MalformedInputException> { originalStrXodusPropStyle.encodeToByteArray(throwOnInvalidSequence = true) }

        // let's migrate such a string from Classic Xodus to Orient and see what happens
        val entities = pileOfEntities(
            eProps("type1", 1, "prop1" to originalStrXodusPropStyle)
        )
        xodus.withTx { tx -> tx.createEntities(entities) }
        migrateAndCheckData(xodus.store, orientDb)

        xodus.withTx { xTx ->
            orientDb.withStoreTx { oTx ->
                val xStr = xTx.getAll("type1").first().getProperty("prop1") as String
                val oStr = oTx.getAll("type1").first().getProperty("prop1") as String

                // string in Xodus equals to the original string
                assertEquals(originalStrXodusPropStyle, xStr)
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
            eProps("type1", 4, "prop1" to getBrokenXodusString(), "prop2" to true, "prop3" to 3.3),

            eProps("type2", 2, "pop1" to "four", "pop2" to true, "pop3" to 2.2),
            eProps("type2", 4, "pop1" to "five", "pop2" to true, "pop3" to 3.3),
            eProps("type2", 5, "pop1" to "six", "pop2" to true, "pop3" to 4.4),
        )
        xodus.withTx { tx ->
            tx.createEntities(entities)
        }

        migrateAndCheckData(xodus.store, orientDb)

        orientDb.store.executeInTransaction { tx ->
            tx.assertOrientContainsAllTheEntities(entities)
        }
    }

    @Test
    fun `copy sets`() {
        val entities = pileOfEntities(
            eSets("type1", 1, "set1" to setOf(1, 2 ,3), "set2" to setOf(300, 100)),
            eSets("type1", 2, "set1" to setOf("ok string", "not a bad string", getBrokenXodusString())),
        )
        xodus.withTx { tx ->
            tx.createEntities(entities)
        }

        migrateAndCheckData(xodus.store, orientDb)

        orientDb.store.executeInTransaction { tx ->
            tx.assertOrientContainsAllTheEntities(entities)
        }
    }

    @Test
    fun `copy an empty entity`() {
        val entityId = xodus.withTx { tx ->
            tx.newEntity("type1").id
        }

        migrateAndCheckData(xodus.store, orientDb)
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

        migrateAndCheckData(xodus.store, orientDb)

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

        migrateAndCheckData(xodus.store, orientDb)

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

        migrateAndCheckData(xodus.store, orientDb)

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

        migrateAndCheckData(xodus.store, orientDb)

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

        migrateAndCheckData(xodus.store, orientDb)

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

                    for (oEntity in oSession.query("select from $type").vertexStream().map { it as OVertexDocument }) {
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

    private fun migrateAndCheckData(xodus: PersistentEntityStore, orient: InMemoryOrientDB) {
        val stats = migrateDataFromXodusToOrientDb(
            xodus,
            orient.store,
            orient.provider,
            orient.schemaBuddy
        )
        checkDataIsSame(xodus, orient.store, stats.xEntityIdToOEntityId)
    }
}


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
        if (expectedValue is String && expectedValue != actualValue) {
            // expected string may be a Classic Xodus "broken" string
            // fix it and compare again
            val fixedExpectedValue = expectedValue.encodeToByteArray().decodeToString()
            Assert.assertEquals(fixedExpectedValue, actualValue)
        } else {
            Assert.assertEquals(expectedValue, actualValue)
        }
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
    for ((name, set) in entity.sets) {
        e.setProperty(name, set)
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

internal class PileOfEntities {
    private val typeToEntities = mutableMapOf<String, MutableMap<Int, Entity>>()

    val types: Set<String> get() = typeToEntities.keys

    fun add(entity: Entity) {
        typeToEntities.getOrPut(entity.type) { mutableMapOf() }[entity.id] = entity
    }

    fun getAll(type: String): Collection<Entity> = typeToEntities.getValue(type).values

    fun getEntity(type: String, id: Int): Entity = typeToEntities.getValue(type).getValue(id)
}

internal data class Entity(
    val type: String,
    val id: Int,
    val props: Map<String, Comparable<*>> = mapOf(),
    val sets: Map<String, ComparableSet<*>> = mapOf(),
    val blobs: Map<String, String> = mapOf(),
    val stringBlobs: Map<String, String> = mapOf(),
    val links: List<Link> = listOf()
)

internal data class Link(
    val name: String,
    val targetType: String,
    val targetId: Int,
)

internal fun pileOfEntities(vararg entities: Entity): PileOfEntities {
    val pile = PileOfEntities()
    for (entity in entities) {
        pile.add(entity)
    }
    return pile
}

internal fun eProps(type: String, id: Int, vararg props: Pair<String, Comparable<*>>): Entity {
    return Entity(type, id, props.toMap())
}

internal fun <T> eSets(type: String, id: Int, vararg sets: Pair<String, Set<T>>): Entity where T: Comparable<T> {
    val mapOfSets = sets.associate { (name, set) ->
        Pair(name, ComparableSet<T>(set))
    }
    return Entity(type, id, sets = mapOfSets)
}

internal fun eBlobs(type: String, id: Int, vararg blobs: Pair<String, String>): Entity {
    return Entity(type, id, blobs = blobs.toMap())
}

internal fun eStringBlobs(type: String, id: Int, vararg blobs: Pair<String, String>): Entity {
    return Entity(type, id, stringBlobs = blobs.toMap())
}

internal fun eLinks(type: String, id: Int, vararg links: Link): Entity {
    return Entity(type, id, links = links.toList())
}

private fun String.toBytesXodusPropertyStyle(dropEOF: Boolean = false): ByteArray {
    val stream = LightOutputStream()
    StringBinding.BINDING.writeObject(stream, this)
    val size = if (dropEOF) stream.size() - 1 else stream.size()
    return ByteArray(size) {
        stream.bufferBytes[it]
    }
}

private fun ByteArray.toStringXodusPropertyStyle(addEOF: Boolean = false): String {
    val arr = if (addEOF) {
        val newArr = ByteArray(this.size + 1)
        this.copyInto(newArr)
        newArr[newArr.lastIndex] = 0
        newArr
    } else this
    return StringBinding.BINDING.readObject(ByteArraySizedInputStream(arr))
}

private fun getBrokenXodusString(): String {
    val rawBytesGottenFromAProdXodus = "82e197b0cf83c4a7e2b1a2ceb1c59fc4a7ceb96d20e1b9a8c4a7ceb1e1b8adeda080c4a700".hexToByteArray()
    val originalBytesForXodus = rawBytesGottenFromAProdXodus.sliceArray(1 until rawBytesGottenFromAProdXodus.size)
    return originalBytesForXodus.toStringXodusPropertyStyle()
}

private fun randomUtf8String(@Suppress("SameParameterValue") size: Int): String = buildString {
    while (this.length < size) {
        val char = Random.nextInt(0, 0xFFFF).toChar()
        if (!char.isSurrogate()) {
            append(char)
        }
    }
}

