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
package jetbrains.exodus.entitystore

import com.orientechnologies.orient.core.db.ODatabaseType
import jetbrains.exodus.entitystore.orientdb.ODatabaseConfig
import jetbrains.exodus.entitystore.orientdb.initOrientDbServer
import java.nio.file.Files
import kotlin.io.path.absolutePathString
import kotlin.test.Test

class WrongUsernameTest {

    @Test(expected = IllegalArgumentException::class)
    fun cannotCreateDatabaseWithWrongUsername() {
        val cfg = ODatabaseConfig
            .builder()
            .withUserName(";drop database users")
            .withDatabaseType(ODatabaseType.MEMORY)
            .withDatabaseName("hello")
            .withDatabaseRoot(Files.createTempDirectory("haha").absolutePathString())
            .build()
        initOrientDbServer(cfg)
    }


}
