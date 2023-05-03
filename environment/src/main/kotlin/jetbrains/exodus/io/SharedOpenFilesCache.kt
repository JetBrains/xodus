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
package jetbrains.exodus.io

import jetbrains.exodus.ExodusException
import jetbrains.exodus.core.dataStructures.ObjectCache
import jetbrains.exodus.util.SharedRandomAccessFile
import java.io.File
import java.io.IOException

class SharedOpenFilesCache private constructor(openFiles: Int) {
    private val cache: ObjectCache<File, SharedRandomAccessFile>

    init {
        cache = ObjectCache(openFiles)
    }

    @Throws(IOException::class)
    fun getCachedFile(file: File): SharedRandomAccessFile {
        var result = cache.newCriticalSection().use { _ ->
            val loadResult = cache.tryKey(file)
            if (loadResult != null && loadResult.employ() > 1) {
                loadResult.close()
                null
            } else {
                loadResult
            }
        }

        if (result == null) {
            result = openFile(file)
            var obsolete: SharedRandomAccessFile? = null
            cache.newCriticalSection().use {
                if (cache.getObject(file) == null) {
                    result.employ()
                    obsolete = cache.cacheObject(file, result)
                }
            }
            if (obsolete != null) {
                obsolete!!.close()
            }
        }

        return result
    }

    @Throws(IOException::class)
    fun openFile(file: File): SharedRandomAccessFile {
        return SharedRandomAccessFile(file, "r")
    }

    @Throws(IOException::class)
    fun removeFile(file: File) {
        val result: SharedRandomAccessFile?
        cache.newCriticalSection().use { result = cache.remove(file) }
        result?.close()
    }

    @Throws(IOException::class)
    fun removeDirectory(dir: File) {
        val result: MutableList<SharedRandomAccessFile> = ArrayList()
        val obsoleteFiles: MutableList<File> = ArrayList()
        cache.newCriticalSection().use {
            val keys = cache.keys()
            while (keys.hasNext()) {
                val file = keys.next()
                if (file.parentFile == dir) {
                    obsoleteFiles.add(file)
                    result.add(cache.getObject(file))
                }
            }
            for (file in obsoleteFiles) {
                cache.remove(file)
            }
        }
        for (obsolete in result) {
            obsolete.close()
        }
    }

    @Throws(IOException::class)
    private fun clear() {
        val openFiles: MutableList<SharedRandomAccessFile> = ArrayList()
        cache.newCriticalSection().use {
            val values = cache.values()
            while (values.hasNext()) {
                openFiles.add(values.next())
            }
            cache.clear()
        }
        for (file in openFiles) {
            file.close()
        }
    }

    companion object {
        private val syncObject = Any()
        private var cacheSize = 0

        @Volatile
        private var theCache: SharedOpenFilesCache? = null

        @JvmStatic
        fun setSize(cacheSize: Int) {
            require(cacheSize > 0) { "Cache size must be a positive integer value" }
            Companion.cacheSize = cacheSize
        }

        @JvmStatic
        val instance: SharedOpenFilesCache
            get() {
                if (cacheSize == 0) {
                    throw ExodusException("Size of SharedOpenFilesCache is not set")
                }
                var result = theCache
                if (result == null) {
                    synchronized(syncObject) {
                        result = theCache
                        if (result == null) {
                            theCache =
                                SharedOpenFilesCache(cacheSize)
                            result = theCache
                        }
                    }
                }
                return result!!
            }

        /**
         * For tests only!!!
         */
        @Throws(IOException::class)
        @JvmStatic
        fun invalidate() {
            val obsolete: SharedOpenFilesCache?
            synchronized(syncObject) {
                obsolete = theCache
                theCache = null
            }
            obsolete?.clear()
        }
    }
}
