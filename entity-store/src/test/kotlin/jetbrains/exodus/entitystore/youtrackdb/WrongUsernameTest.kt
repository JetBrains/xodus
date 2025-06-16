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

import YTDBDatabaseProviderFactory
import com.jetbrains.youtrack.db.api.DatabaseType
import org.junit.Test
import java.nio.file.Files
import kotlin.io.path.absolutePathString

class WrongUsernameTest {

    @Test(expected = IllegalArgumentException::class)
    fun cannotCreateDatabaseWithWrongUsername() {
        val params = YTDBDatabaseParams.builder()
            .withDatabasePath(Files.createTempDirectory("haha").absolutePathString())
            .withAppUser(";drop database users", "hello")
            .withDatabaseType(DatabaseType.MEMORY)
            .withDatabaseName("hello")
            .build()
        YTDBDatabaseProviderFactory.createProvider(params)
    }
}
