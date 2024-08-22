/*
 * Copyright 2010 - 2024 JetBrains s.r.o.
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
package jetbrains.exodus.env.imp

import jetbrains.exodus.env.EnvExportImport
import jetbrains.exodus.env.EnvironmentConfig
import java.nio.file.Paths
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    if (args.size < 2) {
        printUsage()
    }

    val envPath = args[0]
    val importPath = args[1]

    EnvExportImport.importEnvironment(Paths.get(envPath), EnvironmentConfig(), Paths.get(importPath))

}

private fun printUsage() {
    println("Usage: import <database path> <import file path>")
    exitProcess(1)
}