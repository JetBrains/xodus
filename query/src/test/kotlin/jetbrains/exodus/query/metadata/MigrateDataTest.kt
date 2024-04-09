package jetbrains.exodus.query.metadata

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
    fun trista() {
        val typeToEntities = mapOf(
            "type1" to listOf<Map<String, Comparable<*>>>(
                mapOf("id" to 1, "prop1" to "one", "prop2" to true, "prop3" to 1.1),
                mapOf("id" to 2, "prop1" to "two", "prop2" to true, "prop3" to 2.2),
                mapOf("id" to 3, "prop1" to "three", "prop2" to true, "prop3" to 3.3),
            ),
            "type2" to listOf<Map<String, Comparable<*>>>(
                mapOf("id" to 4, "pop1" to "four", "pop2" to true, "pop3" to 4.4),
                mapOf("id" to 5, "pop1" to "five", "pop2" to true,  "pop3" to 5.5),
                mapOf("id" to 6, "pop1" to "six",  "pop2" to true, "pop3" to 6.6),
            )
        )
        xodus.withTx { tx ->
            for ((type, entities) in typeToEntities) {
                for (item in entities) {
                    tx.createEntity(type, item)
                }
            }
        }

        XodusToOrientDataMigrator(xodus.store, orientDb.store).migrate()

        orientDb.withSession { oSession ->
            for (type in typeToEntities.keys) {
                for (record in oSession.browseClass(type)) {
                    val id = record.getProperty<Comparable<*>>("id")
                    val entity = typeToEntities.getValue(type).first { it["id"] == id }
                    for ((propName, propValue) in entity.entries) {
                        Assert.assertEquals(propValue, record.getProperty<Comparable<*>>(propName))
                    }
                }
            }
        }
    }

    private fun StoreTransaction.createEntity(type: String, props: Map<String, Comparable<*>>) {
        val e = this.newEntity(type)
        for ((name, value) in props) {
            e.setProperty(name, value)
        }
    }
}