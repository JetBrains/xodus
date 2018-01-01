/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.vfs

import jetbrains.exodus.env.Environments
import java.nio.file.Paths
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    if (args.isEmpty()) {
        printUsage()
    }
    var envPath: String? = null
    var targetDirectory: String? = null
    var hasOptions = false
    for (arg in args) {
        if (arg.startsWith('-')) {
            when (arg.toLowerCase().substring(1)) {
                "d" -> hasOptions = true

                else -> {
                    printUsage()
                }
            }
        } else {
            if (envPath == null) {
                envPath = arg
            } else {
                targetDirectory = arg
                break
            }
        }
    }
    if (!hasOptions || envPath == null || targetDirectory == null) {
        printUsage()
    }
    val vfs = VirtualFileSystem(Environments.newInstance(envPath!!))
    vfs.environment.executeInReadonlyTransaction { txn ->
        vfs.dump(txn, Paths.get(targetDirectory!!))
    }
    vfs.shutdown()
}

internal fun printUsage() {
    println("Usage: Vfs [-options] <environment path> <target directory>")
    println("Options:")
    println("  -d              dump Virtual File System in specified environment to specified directory")
    exitProcess(1)
}