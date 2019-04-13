package jetbrains.exodus.io

import jetbrains.exodus.ExodusException
import jetbrains.exodus.OutOfDiskSpaceException
import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import java.lang.management.ManagementFactory
import java.nio.channels.FileLock
import java.nio.channels.OverlappingFileLockException
import java.util.*

class LockingManager internal constructor(private val dir: File, private val lockId: String?) {

    private var lockFile: RandomAccessFile? = null
    private var lock: FileLock? = null

    val usableSpace: Long
        get() = getLockFile().usableSpace

    fun lock(timeout: Long): Boolean {
        val started = System.currentTimeMillis()
        do {
            if (lock()) return true
            if (timeout > 0) {
                try {
                    Thread.sleep(100)
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    break
                }
            }
        } while (System.currentTimeMillis() - started < timeout)
        return false
    }

    fun release(): Boolean {
        if (lockFile != null) {
            try {
                close()
                return true
            } catch (e: IOException) {
                throw ExodusException("Failed to release lock file $LOCK_FILE_NAME", e)
            }
        }
        return false
    }

    private fun lock(): Boolean {
        if (lockFile != null) return false // already locked!
        try {
            val lockFileHandle = getLockFile()
            val lockFile = RandomAccessFile(lockFileHandle, "rw")
            this.lockFile = lockFile
            val channel = lockFile.channel
            lock = channel.tryLock()
            if (lock != null) {
                lockFile.setLength(0)
                lockFile.writeBytes("Private property of Exodus: ")
                if (lockId == null) {
                    val bean = ManagementFactory.getRuntimeMXBean()
                    if (bean != null) {
                        // Got runtime system bean (try to get PID)
                        // Result of bean.getName() is unknown
                        lockFile.writeBytes(bean.name)
                    }
                } else {
                    lockFile.writeBytes("$lockId")
                }
                lockFile.writeBytes("\n\n")
                for (element in Throwable().stackTrace) {
                    lockFile.writeBytes(element.toString() + '\n')
                }
                channel.force(false)
            }
        } catch (e: IOException) {
            try {
                close()
            } catch (_: IOException) {
                //throw only first cause
            }
            return throwFailedToLock(e)
        } catch (_: OverlappingFileLockException) {
            try {
                close()
            } catch (_: IOException) {
                //throw only first cause
            }
        }
        if (lock == null) {
            try {
                close()
            } catch (e: IOException) {
                throwFailedToLock(e)
            }
        }
        return lockFile != null
    }

    fun lockInfo(): String? {
        try {
            // "stupid scanner trick" for reading entire file in a string (https://community.oracle.com/blogs/pat/2004/10/23/stupid-scanner-tricks)
            return with(Scanner(getLockFile()).useDelimiter("\\A")) {
                if (hasNext()) next() else null
            }
        } catch (e: IOException) {
            throw ExodusException("Failed to read contents of lock file $LOCK_FILE_NAME", e)
        }
    }

    private fun getLockFile(): File {
        return File(dir, LOCK_FILE_NAME)
    }

    private fun throwFailedToLock(e: IOException): Boolean {
        if (usableSpace < 4096) {
            throw OutOfDiskSpaceException(e)
        }
        throw ExodusException("Failed to lock file $LOCK_FILE_NAME", e)
    }

    @Throws(IOException::class)
    private fun close() {
        lock?.release()
        lockFile?.apply {
            close()
            lockFile = null
        }
    }

    companion object {

        private const val LOCK_FILE_NAME = "xd.lck"
    }
}