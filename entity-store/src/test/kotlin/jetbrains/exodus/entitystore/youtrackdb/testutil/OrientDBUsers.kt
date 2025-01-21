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

package jetbrains.exodus.entitystore.youtrackdb.testutil

import jetbrains.exodus.entitystore.youtrackdb.YTDBStoreTransaction
import jetbrains.exodus.entitystore.youtrackdb.YTDBVertexEntity

object BaseUser {
    const val CLASS = "BaseUser"

    object Props {
        const val NAME = "login"
    }
}

object Guest {
    const val CLASS = "Guest"
}

object User {
    const val CLASS = "User"
}

object Agent {
    const val CLASS = "Agent"
}

object Admin {
    const val CLASS = "Admin"
}


fun YTDBStoreTransaction.createUser(userClass: String, name: String): YTDBVertexEntity {
    return newEntity(userClass).apply {
        setProperty("name", name)
    } as YTDBVertexEntity
}
