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
package jetbrains.exodus.javascript

import jetbrains.exodus.core.crypto.MessageDigestUtil
import jetbrains.exodus.core.execution.ThreadJobProcessorPool
import jetbrains.exodus.javascript.RhinoCommand.Companion.CONSOLE
import mu.KLogging
import org.apache.sshd.common.keyprovider.FileKeyPairProvider
import org.apache.sshd.server.SshServer
import org.apache.sshd.server.auth.password.PasswordAuthenticator
import java.io.Closeable
import java.nio.file.Paths

class RhinoServer(config: Map<String, *>, port: Int = 2808, password: String? = null) : Closeable {

    companion object : KLogging() {

        val commandProcessor = ThreadJobProcessorPool.getOrCreateJobProcessor("Exodus shared RhinoServer processor")
    }

    private val sshd: SshServer

    init {
        val passwordMD5 = password ?: MessageDigestUtil.MD5(password)
        logger.info {
            "Starting SSH daemon on port $port " +
                    if (password == null) "with anonymous access" else ", password hash = $passwordMD5"
        }
        sshd = SshServer.setUpDefaultServer().apply {
            keyPairProvider = FileKeyPairProvider(Paths.get(System.getProperty("user.home"), ".xodus.ser"))
            passwordAuthenticator = PasswordAuthenticator { _, password, _ ->
                passwordMD5 == null || passwordMD5 == MessageDigestUtil.MD5(password)
            }
            setShellFactory { RhinoCommand.createCommand(config) }
            setPort(port)
            // if we 're within console then setup infinite session timeout
            if (config[CONSOLE] == true) {
                properties["idle-timeout"] = Long.MAX_VALUE.toString()
            }
            start()
        }
    }

    val port: Int = sshd.port

    override fun close() {
        logger.info {
            "Stopping SSH daemon ${sshd.host}:${sshd.port}"
        }
        sshd.stop()
    }
}