package jetbrains.exodus.crypto

import jetbrains.exodus.entitystore.PersistentEntityStoreImpl
import jetbrains.exodus.env.Reflect
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream
import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream
import java.util.zip.GZIPOutputStream

fun main(args: Array<String>) {
    if (args.size < 2) {
        printUsage()
    }
    var sourcePath: String? = null
    var targetPath: String? = null
    var key: ByteArray? = null
    var compress = false
    var overwirte = false

    for (arg in args) {
        if (arg.startsWith('-')) {
            when (arg.toLowerCase().substring(1)) {
                "z" -> compress = true
                "o" -> overwirte = true
                else -> {
                    printUsage()
                    return
                }
            }
        } else {
            if (sourcePath == null) {
                sourcePath = arg
            } else if (targetPath == null) {
                targetPath = arg
            } else if (key == null) {
                key = toBinaryKey(arg)
                break
            }
        }
    }

    if (sourcePath == null || targetPath == null || key == null) {
        printUsage()
        return
    }

    val source = File(sourcePath)
    val target = File(targetPath)

    if (!source.exists()) {
        println("File not found: ${source.absolutePath}")
        return
    }

    if (target.exists()) {
        if (!overwirte) {
            println("File exists: ${target.absolutePath}")
            return
        }
        if (!target.deleteRecursively()) {
            println("File cannot be fully deleted: ${target.absolutePath}")
            return
        }
    }

    if (source.isDirectory && compress) {
        val env = Reflect.openEnvironment(source)
        val store = PersistentEntityStoreImpl(env, "ignored")
        val archive = TarArchiveOutputStream(GZIPOutputStream(BufferedOutputStream(FileOutputStream(target))))
        encryptBackupable(key, store, archive)
    }
}

private fun printUsage() {
    println("Usage: Scytale [options] source target key")
    println("Source can be archive or file")
}
