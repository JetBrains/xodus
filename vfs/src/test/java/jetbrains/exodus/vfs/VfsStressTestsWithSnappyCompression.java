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
package jetbrains.exodus.vfs;

import io.airlift.compress.snappy.SnappyCompressor;
import io.airlift.compress.snappy.SnappyDecompressor;
import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class VfsStressTestsWithSnappyCompression extends VfsStressTests {

    @Override
    @Nullable ClusterConverter getClusterConverter() {
        return new ClusterConverter() {
            @NotNull
            @Override
            public ByteIterable onRead(@NotNull final ByteIterable raw) {
                final int length = raw.getLength();
                int decompressedLength = length * 3 + 500;
                final byte[] result = new byte[decompressedLength];
                decompressedLength = new SnappyDecompressor().decompress(
                    raw.getBytesUnsafe(), 0, length, result, 0, decompressedLength);
                return new ArrayByteIterable(result, decompressedLength);
            }

            @NotNull
            @Override
            public ByteIterable onWrite(@NotNull final ByteIterable source) {
                final int sourceLength = source.getLength();
                int compressedLength = sourceLength + sourceLength / 5 + 50;
                final byte[] compressed = new byte[compressedLength];
                compressedLength = new SnappyCompressor().compress(
                    source.getBytesUnsafe(), 0, sourceLength, compressed, 0, compressedLength);
                return new ArrayByteIterable(compressed, compressedLength);
            }
        };
    }
}
