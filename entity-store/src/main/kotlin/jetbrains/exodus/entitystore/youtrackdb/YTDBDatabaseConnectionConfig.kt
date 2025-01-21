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

import com.jetbrains.youtrack.db.api.DatabaseType

class YTDBDatabaseConnectionConfig private constructor(
    val databaseRoot: String,
    val userName: String,
    val password: String,
    val databaseType: DatabaseType,
    val closeAfterDelayTimeout: Int
) {
    companion object {
        fun builder(): Builder {
            return Builder()
        }
    }

    @Suppress("unused")
    class Builder internal constructor() {
        private lateinit var databaseRoot: String
        private lateinit var userName: String
        private lateinit var password: String
        private var databaseType: DatabaseType = DatabaseType.MEMORY
        private var closeAfterDelayTimeout: Int = 10

        fun withDatabaseRoot(databaseURL: String) = apply { this.databaseRoot = databaseURL }
        fun withUserName(userName: String) = apply { this.userName = userName }
        fun withPassword(password: String) = apply { this.password = password }
        fun withDatabaseType(databaseType: DatabaseType) = apply { this.databaseType = databaseType }
        fun withCloseAfterDelayTimeout(closeAfterDelayTimeout: Int) =
            apply { this.closeAfterDelayTimeout = closeAfterDelayTimeout }

        fun build() = YTDBDatabaseConnectionConfig(
            databaseRoot, userName, password, databaseType,
            closeAfterDelayTimeout
        )
    }
}
