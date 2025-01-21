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
package jetbrains.exodus.entitystore.youtrackdb

import org.junit.Assert
import org.junit.Test

class YTDBDatabaseConfigTest {
    @Test
    fun `cypher key is trunked to 24 bytes from bigger one`() {
        val key1 = Array(60) { "aa" }.joinToString(separator = "")

        val connConfig = YTDBDatabaseConnectionConfig
            .builder()
            .withUserName("testUrl")
            .withPassword("testPassword")
            .withDatabaseRoot("aa")
            .build()


        val cfg = YTDBDatabaseConfig
            .builder()
            .withConnectionConfig(connConfig)
            .withStringHexAndIV(key1, 10L)
            .withDatabaseName("aa")
            .build()

        Assert.assertEquals(24, cfg.cipherKey?.size)
    }

    @Test
    fun `cypher key is not trunked if key is smaller than 24`() {
        val key1 = "aabbccddaabbccdd"

        val connConfig = YTDBDatabaseConnectionConfig.builder()
            .withUserName("testUrl")
            .withPassword("testPassword")
            .withDatabaseRoot("aa")
            .build()

        val cfg = YTDBDatabaseConfig
            .builder()
            .withConnectionConfig(connConfig)
            .withStringHexAndIV(key1, 10L)
            .withDatabaseName("aa")

            .build()

        Assert.assertEquals(16, cfg.cipherKey?.size)
    }

}
