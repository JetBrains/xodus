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
package jetbrains.exodus

import jetbrains.exodus.javascript.RhinoCommand.Companion.ENTITY_STORES
import jetbrains.exodus.javascript.RhinoCommand.Companion.ENVIRONMENTS
import jetbrains.exodus.javascript.ptyShell
import jetbrains.exodus.javascript.startRhinoServer
import java.util.logging.LogManager
import kotlin.system.exitProcess

fun main(args: Array<String>) {

    // disable logging to stdout
    LogManager.getLogManager().reset()

    if (args.isEmpty()) {
        printUsage()
    }
    when (args[0].toLowerCase()) {
        "reflect" -> jetbrains.exodus.env.main(args.skipFirst)
        "scytale" -> jetbrains.exodus.crypto.main(args.skipFirst)
        "vfs" -> jetbrains.exodus.vfs.main(args.skipFirst)
        "environmentjsconsole" ->
            startRhinoServer(args.skipFirst, ENVIRONMENTS).use {
                ptyShell(it.port)
            }
        "entitystorejsconsole" ->
            startRhinoServer(args.skipFirst, ENTITY_STORES).use {
                ptyShell(it.port)
            }
        else -> printUsage()
    }
    exitProcess(0)
}

internal fun printUsage() {
    println("Usage: <tool name> [tool parameters]")
    println("Available tools: Reflect | Scytale | Vfs | EnvironmentJSConsole | EntityStoreJSConsole")
    exitProcess(1)
}

internal val Array<String>.skipFirst: Array<String>
    get() = if (this.size > 1) this.copyOfRange(1, this.size) else arrayOf()