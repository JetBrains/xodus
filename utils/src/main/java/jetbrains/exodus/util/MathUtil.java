/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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
package jetbrains.exodus.util;

public class MathUtil {

    /**
     * @param i integer
     * @return discrete logarithm of specified integer base 2
     */
    public static int integerLogarithm(final int i) {
        return i <= 0 ? 0 : Integer.SIZE - Integer.numberOfLeadingZeros(i - 1);
    }

    /**
     * @param l long
     * @return discrete logarithm of specified integer base 2
     */
    public static long longLogarithm(final long l) {
        return l <= 0 ? 0 : Long.SIZE - Long.numberOfLeadingZeros(l - 1);
    }
}
