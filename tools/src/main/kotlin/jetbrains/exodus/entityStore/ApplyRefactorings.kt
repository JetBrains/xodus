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
package jetbrains.exodus.entityStore

import jetbrains.exodus.entitystore.PersistentEntityStoreImpl
import jetbrains.exodus.env.reflect.Reflect
import java.io.File

fun main(args: Array<String>) {
    if (args.isEmpty()) {
        printUsage()
        return
    }

    val dir = File(args[0])
    val name = if (args.size > 1) {
        args[1]
    } else {
        "teamsysstore"
    }
    PersistentEntityStoreImpl(Reflect.openEnvironment(dir, false), name).apply {
        close()
    }
}

internal fun printUsage() {
    println("Usage: Refactorings <environment path> [store name]")
    println("       default store name is teamsysstore")
}
