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
    fun `encryption key calculated from hex`() {
        val keyHex = "546e6f624b737371796f41586e7269304c744f42663252613630586631374a67"

        val params = YTDBDatabaseParams.builder()
            .withDatabasePath("aa")
            .withDatabaseName("aa")
            .withHexEncryptionKey(keyHex, 0)
            .build()

        Assert.assertEquals("VG5vYktzc3F5b0FYbnJpMAAAAAAAAAAA", params.encryptionKey)
    }

    @Test
    fun `encryption key is trunked to 32 from bigger one`() {
        val key1 = Array(60) { "aa" }.joinToString(separator = "")

        val params = YTDBDatabaseParams.builder()
            .withDatabasePath("aa")
            .withDatabaseName("aa")
            .withHexEncryptionKey(key1, 0)
            .build()

        Assert.assertEquals(32, params.encryptionKey?.length)
    }

    @Test
    fun `encryption key is not trunked if key is smaller than 32`() {
        val key1 = "aabbccddaabbccdd"

        val params = YTDBDatabaseParams.builder()
            .withDatabasePath("aa")
            .withDatabaseName("aa")
            .withHexEncryptionKey(key1, 0)
            .build()

        Assert.assertEquals(24, params.encryptionKey?.length)
    }
}
