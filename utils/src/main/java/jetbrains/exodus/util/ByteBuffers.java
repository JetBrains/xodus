/*
 * *
 *  * Copyright 2010 - 2022 JetBrains s.r.o.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * https://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package jetbrains.exodus.util;

import java.nio.ByteBuffer;

public final class ByteBuffers {
    public static ByteBuffer mergeBuffers(ByteBuffer first, ByteBuffer second) {
        var result = ByteBuffer.allocate(first.limit() + second.limit());
        var firstSize = first.limit();
        var secondSize = second.limit();

        result.put(0, first, 0, firstSize);
        result.put(firstSize, second, 0, secondSize);

        return result;
    }
}
