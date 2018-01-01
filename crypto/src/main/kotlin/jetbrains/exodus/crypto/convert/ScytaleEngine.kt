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
package jetbrains.exodus.crypto.convert

import jetbrains.exodus.crypto.StreamCipherProvider
import jetbrains.exodus.crypto.asHashedIV
import jetbrains.exodus.log.LogUtil
import jetbrains.exodus.util.ByteArraySpinAllocator
import mu.KLogging
import java.io.Closeable
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit

class ScytaleEngine(
        private val listener: EncryptListener,
        private val cipherProvider: StreamCipherProvider,
        private val key: ByteArray,
        private val basicIV: Long,
        private val blockAlignment: Int = LogUtil.LOG_BLOCK_ALIGNMENT,
        bufferSize: Int = 1024 * 1024,  // 1MB
        inputQueueSize: Int = 40,
        outputQueueSize: Int = 40
) : Closeable {
    companion object : KLogging() {
        val timeout = 200L
    }

    private val inputQueue = ArrayBlockingQueue<EncryptMessage>(inputQueueSize)
    private val outputQueue = ArrayBlockingQueue<EncryptMessage>(outputQueueSize)
    private val bufferAllocator = ByteArraySpinAllocator(bufferSize, inputQueueSize + outputQueueSize + 4)

    @Volatile
    private var producerFinished = false
    @Volatile
    private var consumerFinished = false
    @Volatile
    private var cancelled = false
    @Volatile
    var error: Throwable? = null

    private val statefulProducer = object : Runnable {
        private val cipher = cipherProvider.newCipher()
        private var offset = 0
        private var iv = 0L

        override fun run() {
            try {
                while (!cancelled && error == null) {
                    inputQueue.poll(timeout, TimeUnit.MILLISECONDS)?.let {
                        when (it) {
                            is FileHeader -> {
                                offset = 0
                                iv = basicIV + if (it.chunkedIV) {
                                    it.handle / blockAlignment
                                } else {
                                    it.handle
                                }
                                cipher.init(key, iv.asHashedIV())
                            }
                            is FileChunk -> encryptChunk(it)
                            is EndChunk -> Unit
                            else -> throw IllegalArgumentException()
                        }
                        while (!outputQueue.offer(it, timeout, TimeUnit.MILLISECONDS)) {
                            if (cancelled || error != null) {
                                return
                            }
                        }
                    } ?: if (producerFinished) {
                        return
                    }
                }
            } catch (t: Throwable) {
                producerFinished = true
                error = t
            }
        }

        private fun encryptChunk(it: FileChunk) {
            if (it.header.canBeEncrypted) {
                val data = it.data
                if (it.header.chunkedIV) {
                    blockEncrypt(it.size, data)
                } else {
                    encrypt(it.size, data)
                }
            }
        }

        private fun encrypt(size: Int, data: ByteArray) {
            for (i in 0 until size) {
                data[i] = cipher.crypt(data[i])
            }
        }

        private fun blockEncrypt(size: Int, data: ByteArray) {
            for (i in 0 until size) {
                data[i] = cipher.crypt(data[i])
                if (++offset == blockAlignment) {
                    offset = 0
                    cipher.init(key, (++iv).asHashedIV())
                }
            }
        }
    }

    private val producer = Thread(statefulProducer, "xodus encrypt " + hashCode())

    private val consumer = Thread({
        try {
            var currentFile: FileHeader? = null
            while (!cancelled && error == null) {
                outputQueue.poll(timeout, TimeUnit.MILLISECONDS)?.let {
                    when (it) {
                        is FileHeader -> {
                            currentFile?.let {
                                listener.onFileEnd(it)
                            }
                            currentFile = it
                            listener.onFile(it)
                        }
                        is FileChunk -> {
                            val current = currentFile
                            if (current != null && current != it.header) {
                                throw Throwable("Invalid chunk with header " + it.header.path)
                            } else {
                                listener.onData(it.header, it.size, it.data)
                                bufferAllocator.dispose(it.data)
                            }
                        }
                        is EndChunk -> {
                            currentFile?.let {
                                listener.onFileEnd(it)
                            }
                        }
                        else -> throw IllegalArgumentException()
                    }
                } ?: if (consumerFinished) {
                    return@Thread
                }
            }
        } catch (t: Throwable) {
            consumerFinished = true
            error = t
        }
    }, "xodus write " + hashCode())

    fun start() {
        producer.start()
        consumer.start()
    }

    fun alloc(): ByteArray = bufferAllocator.alloc()

    fun put(e: EncryptMessage) {
        while (!inputQueue.offer(e, timeout, TimeUnit.MILLISECONDS)) {
            if (error != null) {
                throw RuntimeException(error)
            }
        }
    }

    fun cancel() {
        cancelled = true
    }

    override fun close() {
        producerFinished = true
        producer.join()
        consumerFinished = true
        consumer.join()
        error?.let { throw RuntimeException(it) }
    }
}
