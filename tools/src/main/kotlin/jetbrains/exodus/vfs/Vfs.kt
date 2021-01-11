/**
 * Copyright 2010 - 2021 JetBrains s.r.o.
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

import jetbrains.exodus.ExodusException
import jetbrains.exodus.env.Environments
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import java.nio.file.Paths
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    if (args.isEmpty()) {
        printUsage()
    }
    var envPath: String? = null
    var targetDirectory: String? = null
    var dOption = false
    var duOption = false
    for (arg in args) {
        if (arg.startsWith('-')) {
            when (arg.toLowerCase().substring(1)) {
                "d" -> dOption = true
                "du" -> duOption = true
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
    if (dOption && envPath != null && targetDirectory != null) {
        dump(envPath, targetDirectory)
        return
    }
    if (duOption && envPath != null) {
        diskUsage(envPath)
        return
    }
    printUsage()
}

private fun dump(envPath: String, targetDirectory: String) {
    openVfsAndExecuteInReadonlyTxn(envPath) { txn ->
        dump(txn, Paths.get(targetDirectory))
    }
}

private fun diskUsage(envPath: String) {
    openVfsAndExecuteInReadonlyTxn(envPath) { txn ->
        println("Disk usage: ${diskUsage(txn)} bytes.")
    }
}

private fun openVfsAndExecuteInReadonlyTxn(envPath: String, action: VirtualFileSystem.(Transaction) -> Unit) {
    val env = Environments.newInstance(envPath)
    val vfs = try {
        VirtualFileSystem(env)
    } catch (e: ExodusException) {
        VirtualFileSystem(env, VfsConfig.DEFAULT, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING)
    }
    env.executeInReadonlyTransaction { txn -> vfs.action(txn) }
    vfs.shutdown()
}

private fun printUsage() {
    println("Usage: Vfs [-options] <environment path> [target directory]")
    println("Options:")
    println("  -d              dump Virtual File System in specified environment to target directory")
    println("  -du             print disk usage of the Virtual File System in specified environment")
    exitProcess(1)
}