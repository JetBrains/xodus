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
package jetbrains.exodus.lucene.codecs

import org.apache.lucene.codecs.FilterCodec
import org.apache.lucene.codecs.StoredFieldsFormat
import org.apache.lucene.codecs.compressing.CompressingStoredFieldsFormat
import org.apache.lucene.codecs.compressing.CompressionMode
import org.apache.lucene.codecs.compressing.Compressor
import org.apache.lucene.codecs.compressing.Decompressor
import org.apache.lucene.codecs.lucene87.Lucene87Codec
import org.apache.lucene.store.DataInput
import org.apache.lucene.store.DataOutput
import org.apache.lucene.util.BytesRef

/**
 * Lucene70Codec with no compression of stored fields.
 */
class Lucene87CodecWithNoFieldCompression : FilterCodec("Lucene70CodecWithNoFieldCompression", Lucene87Codec()) {

    private val flatFieldsFormat: StoredFieldsFormat =
            CompressingStoredFieldsFormat("Lucene50StoredFieldsFlat", NoCompression, 16, 1, 16)

    override fun storedFieldsFormat() = flatFieldsFormat
}

private object NoCompression : CompressionMode() {

    override fun newCompressor() = TrivialCompressor()

    override fun newDecompressor() = TrivialDecompressor()
}

private class TrivialCompressor : Compressor() {

    override fun compress(bytes: ByteArray, off: Int, len: Int, out: DataOutput) =
            out.writeBytes(bytes, off, len)

    override fun close() {}
}

private class TrivialDecompressor : Decompressor() {

    override fun clone() = this

    override fun decompress(input: DataInput, originalLength: Int, offset: Int, length: Int, bytes: BytesRef) {
        if (bytes.bytes.size < originalLength) {
            bytes.bytes = ByteArray(originalLength)
        }
        input.readBytes(bytes.bytes, 0, originalLength)
        bytes.offset = 0
        bytes.length = originalLength
    }
}