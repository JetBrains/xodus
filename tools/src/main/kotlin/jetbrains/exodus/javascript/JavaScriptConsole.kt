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
package jetbrains.exodus.javascript

import jetbrains.exodus.core.dataStructures.hash.HashMap
import jetbrains.exodus.javascript.RhinoCommand.Companion.API_LAYER
import jetbrains.exodus.javascript.RhinoCommand.Companion.CONSOLE
import net.schmizz.sshj.SSHClient
import net.schmizz.sshj.common.LoggerFactory
import net.schmizz.sshj.common.StreamCopier
import net.schmizz.sshj.transport.verification.PromiscuousVerifier

const val DEFAULT_PORT = 2808

/**
 * Returns port number which the Rhino server is started on.
 */
fun startRhinoServer(args: Array<String>, apiLayer: String): RhinoServer {
    var port = DEFAULT_PORT
    args.forEach {
        if (it.length > 2 && it.startsWith("-p", ignoreCase = true)) {
            port = it.substring(2).toInt()
            return@forEach
        }
    }
    return RhinoServer(HashMap<String, Any>().apply {
        this[API_LAYER] = apiLayer
        this[CONSOLE] = true
    }, port)
}

/**
 * Create pseudo terminal and the shell for "ssh://localhost:port".
 */
fun ptyShell(port: Int) {
    SSHClient().use { ssh ->
        System.console()
        ssh.addHostKeyVerifier(PromiscuousVerifier())
        ssh.connect("localhost", port)
        ssh.authPassword("", "")
        ssh.startSession().use { session ->
            session.allocateDefaultPTY()
            val shell = session.startShell()
            val done = StreamCopier(shell.inputStream, System.out, LoggerFactory.DEFAULT).spawnDaemon("stdout")
            StreamCopier(shell.errorStream, System.err, LoggerFactory.DEFAULT).spawnDaemon("stderr")
            val input = System.`in`
            while (true) {
                val nextByte = input.read()
                if (nextByte < 0 || done.isSet) {
                    break
                }
                shell.outputStream.write(nextByte)
                shell.outputStream.flush()
            }
        }
    }
}