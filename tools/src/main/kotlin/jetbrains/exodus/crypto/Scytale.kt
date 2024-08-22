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
package jetbrains.exodus.crypto

import jetbrains.exodus.crypto.convert.*
import jetbrains.exodus.crypto.streamciphers.CHACHA_CIPHER_ID
import jetbrains.exodus.crypto.streamciphers.SALSA20_CIPHER_ID
import jetbrains.exodus.entitystore.PersistentEntityStoreImpl
import jetbrains.exodus.env.reflect.Reflect
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream
import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream
import java.lang.Long.parseLong
import java.util.concurrent.atomic.AtomicLong
import java.util.zip.GZIPOutputStream
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    if (args.size < 2) {
        printUsage()
    }
    var sourcePath: String? = null
    var targetPath: String? = null
    var key: ByteArray? = null
    var keyString: String? = null
    var basicIV: Long? = null
    var compress = false
    var gzip = false
    var overwrite = false
    var verbose = false
    var type = "chacha"

    for (arg in args) {
        if (arg.startsWith('-') && arg.length < 3) {
            when (arg.lowercase().substring(1)) {
                "g" -> gzip = true
                "z" -> compress = true
                "o" -> overwrite = true
                "v" -> verbose = true
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
                keyString = arg
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

    val input = if (source.isDirectory) {
        try {
            val env = Reflect.openEnvironment(source, !overwrite)
            PersistentEntityStoreImpl(env, "ignored")
        } catch (icpe: InvalidCipherParametersException) {
            val env = Reflect.openEnvironment(source, !overwrite,
                    cipherId = cipherId, cipherKey = keyString, cipherBasicIV = basicIV)
            PersistentEntityStoreImpl(env, "ignored")
        }
    } else {
        ArchiveBackupableFactory.newBackupable(source, gzip)
    }

    var archive: TarArchiveOutputStream? = null
    val output = if (compress) {
        archive = TarArchiveOutputStream(GZIPOutputStream(BufferedOutputStream(FileOutputStream(target))))
        ArchiveEncryptListenerFactory.newListener(archive)
    } else {
        target.mkdir()
        DirectoryEncryptListenerFactory.newListener(target)
    }

    val finalOutput = if (verbose) {
        val bytesWritten = AtomicLong()

        object : EncryptListener {
            override fun onFile(header: FileHeader) {
                output.onFile(header)
                println(header.path + header.name)
            }

            override fun onFileEnd(header: FileHeader) {
                output.onFileEnd(header)
                println(String.format("MB written: %.2f", bytesWritten.get().toFloat() / (1024 * 1024)))
            }

            override fun onData(header: FileHeader, size: Int, data: ByteArray) {
                output.onData(header, size, data)
                bytesWritten.addAndGet(size.toLong())
            }
        }
    } else {
        output
    }

    try {
        ScytaleEngine(finalOutput, newCipherProvider(cipherId), key, basicIV).encryptBackupable(input)
    } finally {
        archive?.close()
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
    println("  -v              print verbose progress messages")
    println("  -z              make target an archive")
    println("  -o              overwrite target archive or folder")
    exitProcess(1)
}

private fun abort(message: String): Nothing {
    println(message)
    exitProcess(1)
}
