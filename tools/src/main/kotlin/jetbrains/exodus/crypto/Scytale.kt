/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
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
package jetbrains.exodus.crypto

import jetbrains.exodus.crypto.streamciphers.CHACHA_CIPHER_ID
import jetbrains.exodus.crypto.streamciphers.SALSA20_CIPHER_ID
import jetbrains.exodus.entitystore.util.BackupUtil
import org.apache.commons.compress.archivers.ArchiveEntry
import org.apache.commons.compress.archivers.ArchiveInputStream
import org.apache.commons.compress.archivers.ArchiveStreamFactory
import org.apache.commons.compress.archivers.examples.Expander
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.utils.IOUtils
import java.io.BufferedInputStream
import java.io.File
import java.io.InputStream
import java.lang.Long.parseLong
import java.nio.file.Files
import java.util.*
import java.util.zip.GZIPInputStream
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    if (args.size < 2) {
        printUsage()
    }
    var sourcePath: String? = null
    var targetPath: String? = null
    var key: ByteArray? = null
    var basicIV: Long? = null
    var compress = false
    var gzip = false
    var overwrite = false
    var type = "chacha"

    for (arg in args) {
        if (arg.startsWith('-') && arg.length < 3) {
            when (arg.lowercase().substring(1)) {
                "g" -> gzip = true
                "z" -> compress = true
                "o" -> overwrite = true
                else -> {
                    printUsage()
                }
            }
        } else {
            if (sourcePath == null) {
                sourcePath = arg
            } else if (targetPath == null) {
                targetPath = arg
            } else if (key == null) {
                key = toBinaryKey(arg)
            } else if (basicIV == null) {
                basicIV = parseIV(arg)
            } else {
                type = arg.lowercase()
                break
            }
        }
    }

    if (sourcePath == null || targetPath == null || key == null || basicIV == null) {
        printUsage()
    }

    val cipherId = when (type) {
        "salsa" -> SALSA20_CIPHER_ID
        "chacha" -> CHACHA_CIPHER_ID
        else -> {
            abort("Unknown cipher id: $type")
        }
    }

    val source = File(sourcePath)
    val target = File(targetPath)

    if (!source.exists()) {
        abort("File not found: ${source.absolutePath}")
    }

    if (target.exists()) {
        val files = target.list()
        files?.let {
            if (files.isNotEmpty()) {
                if (!overwrite) {
                    abort("File exists: ${target.absolutePath}")
                }
                if (!target.deleteRecursively()) {
                    abort("File cannot be fully deleted: ${target.absolutePath}")
                }
            }
        } ?: target.let {
            if (!overwrite) {
                println("Invalid file: ${target.absolutePath}")
            }
        }
    }
    val enCrypted = if (source.isDirectory) {
        BackupUtil.reEncryptBackup(FileTreeArchiveInputStream(source), key, basicIV, null, 0, newCipherProvider(cipherId))
    } else {
        val stream = ArchiveStreamFactory().createArchiveInputStream(BufferedInputStream(
            if (gzip) {
                GZIPInputStream(source.inputStream())
            } else {
                source.inputStream()
            }
        ))
        BackupUtil.reEncryptBackup(stream, key, basicIV, null, 0, newCipherProvider(cipherId))
    }

    if (compress) {
        IOUtils.copy(enCrypted, target.outputStream())
    } else {
        Expander().expand(TarArchiveInputStream(GZIPInputStream(enCrypted)), target.toPath())
    }
}

private fun parseIV(arg: String): Long {
    if (arg.startsWith("0x")) {
        return parseLong(arg.substring(2), 16)
    }
    return parseLong(arg)
}

private fun printUsage(): Nothing {
    println("Usage: Scytale [options] source target key basicIV [cipher]")
    println("Source can be archive or folder")
    println("Cipher can be 'Salsa' or 'ChaCha', 'ChaCha' is default")
    println("Options:")
    println("  -g              use gzip compression when opening archive")
    println("  -z              make target an archive")
    println("  -o              overwrite target archive or folder")
    exitProcess(1)
}

private fun abort(message: String): Nothing {
    println(message)
    exitProcess(1)
}

internal class FileTreeArchiveInputStream(private val directory: File) : ArchiveInputStream() {

    private val fileIterator: Iterator<File> = Files.walk(directory.toPath()).map { it.toFile() }.iterator()
    private var currentInputStream:InputStream? = null

    override fun read(): Int {
        val stream = currentInputStream ?: throw IllegalStateException("Virtual entry not found")
        return stream.read()
    }

    override fun getNextEntry(): ArchiveEntry? {
        val nextFile = if (fileIterator.hasNext()){
            fileIterator.next()
        } else {
            return null
        }
        currentInputStream?.close()
        currentInputStream = if (nextFile.isDirectory) null else nextFile.inputStream()
        return VirtualArchiveEntry(directory, nextFile)
    }

    internal class VirtualArchiveEntry(private val root: File, private val file: File) : ArchiveEntry {
        override fun getName() = file.absolutePath.substringAfter(root.absolutePath)

        override fun getSize() = file.length()

        override fun isDirectory() = file.isDirectory

        override fun getLastModifiedDate() = Date(file.lastModified())
    }

}