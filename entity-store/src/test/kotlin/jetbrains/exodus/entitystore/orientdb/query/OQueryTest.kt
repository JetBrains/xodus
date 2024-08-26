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
package jetbrains.exodus.entitystore.orientdb.query

import com.google.common.truth.Truth.assertThat
import org.junit.Test

class OQueryTest {

    @Test
    fun `should select by property`() {
        val query = OClassSelect("Person", OEqualCondition("name", "John"))

        val builder = SqlBuilder()
        query.sql(builder)
        println(builder)
    }

    @Test
    fun `should select by property OR property`() {
        // Given
        val condition = OEqualCondition("name", "John").or(OEqualCondition("project", "Sample"))
        val oQuery = OClassSelect("Person", condition)

        // When
        val query = buildSql(oQuery)

        // Then
        assertThat(query.sql)
            .isEqualTo("SELECT FROM Person WHERE (name = :name0 OR project = :project1)")
    }

    @Test
    fun `should select by property AND property`() {
        // Given
        val condition = OEqualCondition("name", "John").and(OEqualCondition("project", "Sample"))
        val oQuery = OClassSelect("Person", condition)

        // When
        val query = buildSql(oQuery)

        // Then
        assertThat(query.sql)
            .isEqualTo("SELECT FROM Person WHERE (name = :name0 AND project = :project1)")
        assertThat(query.params)
            .containsExactly(
                "name0", "John",
                "project1", "Sample"
            )
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
        val oQuery = OClassSelect("Person", condition)

        // When
        val query = buildSql(oQuery)

        // Then
        assertThat(query.sql)
            .isEqualTo("SELECT FROM Person WHERE ((name = :name0 AND (project = :project1 OR project = :project2)) OR project = :project3)")
        assertThat(query.params)
            .containsExactly(
                "name0", "John",
                "project1", "Sample",
                "project2", "Sample2",
                "project3", "Sample3"
            )
    }

    private fun buildSql(oSql: OSql): SqlQuery {
        val builder = SqlBuilder()
        oSql.sql(builder)
        return builder.build()
    }
}