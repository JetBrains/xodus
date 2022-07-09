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

package jetbrains.exodus;

import java.nio.ByteBuffer;
import java.util.Comparator;

public final class ByteBufferComparator implements Comparator<ByteBuffer> {
    public static final ByteBufferComparator INSTANCE = new ByteBufferComparator();

    @Override
    public int compare(ByteBuffer bufferOne, ByteBuffer bufferTwo) {
        int thisPos = bufferOne.position();
        int thisRem = bufferOne.limit() - thisPos;
        int thatPos = bufferTwo.position();
        int thatRem = bufferTwo.limit() - thatPos;
        int length = Math.min(thisRem, thatRem);

        if (length < 0) {
            return -1;
        } else {
            int i = bufferOne.mismatch(bufferTwo);
            if (i >= 0 && i < length) {
                return Byte.compareUnsigned(bufferOne.get(thisPos + i), bufferTwo.get(thatPos + i));
            }

            return thisRem - thatRem;
        }
    }
}
