/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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
package jetbrains.exodus.log.iterate;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterator;
import org.junit.Assert;
import org.junit.Test;

public class FixedLengthByteIterableTest {

    @Test
    public void test() {
        ByteIterator itr = new CompoundByteIterable(
                new ByteIterable[]{
                        new FixedLengthByteIterable(new ArrayByteIterable(
                                new byte[]{(byte) 1, (byte) 2, (byte) 3}
                        ), 0, 1)
                }
        ).iterator();
        itr.skip(1);
        Assert.assertFalse(itr.hasNext());
    }
}
