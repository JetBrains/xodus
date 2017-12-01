/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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
package jetbrains.exodus.crypto

import jetbrains.exodus.crypto.convert.*
import jetbrains.exodus.crypto.streamciphers.SALSA20_CIPHER_ID
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
    var gzip = false
    var overwirte = false

    for (arg in args) {
        if (arg.startsWith('-')) {
            when (arg.toLowerCase().substring(1)) {
                "g" -> gzip = true
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

    val input = if (source.isDirectory) {
        val env = Reflect.openEnvironment(source)
        PersistentEntityStoreImpl(env, "ignored")
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

    try {
        ScytaleEngine(output, newCipherProvider(SALSA20_CIPHER_ID), key).encryptBackupable(input)
    } finally {
        archive?.close()
    }
}

private fun printUsage() {
    println("Usage: Scytale [options] source target key")
    println("Source can be archive or folder")
    println("Options:")
    println("  -g              use gzip compression when opening archive")
    println("  -z              make target an archive")
    println("  -o              overwrite target archive or folder")
}
