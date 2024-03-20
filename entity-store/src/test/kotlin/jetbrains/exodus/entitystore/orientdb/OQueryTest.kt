package jetbrains.exodus.entitystore.orientdb

import jetbrains.exodus.entitystore.orientdb.query.OAllSelect
import jetbrains.exodus.entitystore.orientdb.query.OEqualCondition
import jetbrains.exodus.entitystore.orientdb.query.and
import jetbrains.exodus.entitystore.orientdb.query.equal
import jetbrains.exodus.entitystore.orientdb.query.or
import org.junit.Test

class OQueryTest {

    @Test
    fun `should select by property`() {
        val query = OAllSelect("Person", OEqualCondition("name", "John"))

        println(query.sql())
        println(query.params())
    }

    @Test
    fun `should select by property OR property`() {
        val condition = OEqualCondition("name", "John").or(OEqualCondition("project", "Sample"))
        val query = OAllSelect("Person", condition)

        println(query.sql())
        println(query.params())
    }

    @Test
    fun `should select by property AND property`() {
        val condition = OEqualCondition("name", "John").and(OEqualCondition("project", "Sample"))
        val query = OAllSelect("Person", condition)

        println(query.sql())
        println(query.params())
    }

    @Test
    fun `should select by property AND (property OR property)`() {
        val condition = or(
            and(
                equal("name", "John"),
                or(equal("project", "Sample"), equal("project", "Sample2")),
            ),
            equal("project", "Sample3")
        )
        val query = OAllSelect("Person", condition)

        println(query.sql())
        println(query.params())
    }
}