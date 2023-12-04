/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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

class WatchingFileDataWriter : AbstractDataWriter() {
    override fun write(b: ByteArray?, off: Int, len: Int): Block = throw UnsupportedOperationException()

    override fun removeBlock(blockAddress: Long, rbt: RemoveBlockType) = throw UnsupportedOperationException()

    @Deprecated("Data files are not designed to be truncated")
    override fun truncateBlock(blockAddress: Long, length: Long) = throw UnsupportedOperationException()
    override fun lock(timeout: Long) = throw UnsupportedOperationException()
    override fun release()= throw UnsupportedOperationException()
    override fun lockInfo() = throw UnsupportedOperationException()

    override fun asyncWrite(b: ByteArray?, off: Int, len: Int) = throw UnsupportedOperationException()

    override fun position() = throw UnsupportedOperationException()

    override fun syncImpl() = throw UnsupportedOperationException()

    override fun closeImpl() = throw UnsupportedOperationException()
    override fun clearImpl() = throw UnsupportedOperationException()
    override fun openOrCreateBlockImpl(address: Long, length: Long) = throw UnsupportedOperationException()
}
