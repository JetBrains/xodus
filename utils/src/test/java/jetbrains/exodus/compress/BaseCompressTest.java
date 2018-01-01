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
package jetbrains.exodus.compress;

import jetbrains.exodus.util.ByteArraySizedInputStream;
import jetbrains.exodus.util.IOUtil;
import jetbrains.exodus.util.LightByteArrayOutputStream;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

public class BaseCompressTest {

    static final String UTF8 = "UTF-8";

    protected static ByteArraySizedInputStream toStream(@Nullable final String s) throws UnsupportedEncodingException {
        return s == null ? null : new ByteArraySizedInputStream(s.getBytes(UTF8));
    }

    protected static String toString(final LightByteArrayOutputStream stream) throws UnsupportedEncodingException {
        return new String(stream.toByteArray(), 0, stream.size(), UTF8);
    }

    protected static String toString(final InputStream source) throws IOException {
        final LightByteArrayOutputStream stream = new LightByteArrayOutputStream();
        IOUtil.copyStreams(source, stream, IOUtil.BUFFER_ALLOCATOR);
        return toString(stream);
    }
}
