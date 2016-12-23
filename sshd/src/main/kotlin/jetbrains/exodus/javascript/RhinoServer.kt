package jetbrains.exodus.javascript

import jetbrains.exodus.core.crypto.MessageDigestUtil
import jetbrains.exodus.core.execution.ThreadJobProcessorPool
import mu.KLogging
import org.apache.sshd.SshServer
import org.apache.sshd.server.PasswordAuthenticator
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider
import java.io.File

class RhinoServer(config: Map<String, *>, port: Int = 2808, password: String? = null) {

    companion object : KLogging() {
        val commandProcessor = ThreadJobProcessorPool.getOrCreateJobProcessor("Exodus shared RhinoServer processor")
    }

    private val sshd: SshServer

    init {
        val passwordMD5 = password ?: MessageDigestUtil.MD5(password)
        logger.info {
            "Starting SSH daemon on port $port " +
                    if (password == null) "with anonymous access" else ", password hash = " + passwordMD5
        }
        sshd = SshServer.setUpDefaultServer().apply {
            keyPairProvider = SimpleGeneratorHostKeyProvider(File(System.getProperty("user.home"), ".xodus.ser").absolutePath)
            passwordAuthenticator = PasswordAuthenticator { username, password, session ->
                passwordMD5 == null || passwordMD5 == MessageDigestUtil.MD5(password)
            }
            setShellFactory { RhinoCommand(config) }
            setPort(port)
            start()
        }
    }

    fun stop() {
        logger.info {
            "Stopping SSH daemon ${sshd.host}:${sshd.port}"
        }
        sshd.stop()
    }
}