/**
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

import jetbrains.exodus.TestUtil
import jetbrains.exodus.util.IOUtil
import jetbrains.exodus.util.SharedRandomAccessFile
import org.junit.Test
import java.io.File

class SharedMappedByteBufferTest {

    @Test
    fun smoke() {
        val dir = TestUtil.createTempDir()
        try {
            SharedRandomAccessFile(File(dir, "file"), "rw").use {
                SharedMappedByteBuffer(it).use { }
                it.write(ByteArray(10))
                SharedMappedByteBuffer(it).use { }
            }
        } finally {
            IOUtil.deleteRecursively(dir)
            IOUtil.deleteFile(dir)
        }
    }
}