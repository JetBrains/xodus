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
package jetbrains.exodus.util;

public class ByteUtil {

    private ByteUtil() {
    }

    public static byte or(byte b1, byte b2) {
        return (byte) (b1 | b2);
    }

    public static byte and(byte b1, byte b2) {
        return (byte) (b1 & b2);
    }

    public static short or(short b1, short b2) {
        return (short) (b1 | b2);
    }

    public static short and(short b1, short b2) {
        return (short) (b1 & b2);
    }

    public static short xor(short b1, short b2) {
        return (short) (b1 ^ b2);
    }

    public static short massOr(short... s) {
        short mask = 0;
        for (short i0 : s) {
            mask |= i0;
        }
        return mask;
    }

    public static int shiftLeft(int operand, int step) {
        return operand << step;
    }

    public static short shiftLeft(short operand, short step) {
        return (short) (operand << step);
    }

    public static int xor(int i1, int i2) {
        return i1 ^ i2;
    }

    public static int and(int i1, int i2) {
        return i1 & i2;
    }

    public static int or(int i1, int i2) {
        return i1 | i2;
    }

    public static int not(int i) {
        return ~i;
    }

    public static int massOr(int... i) {
        int mask = 0;
        for (int i0 : i) {
            mask |= i0;
        }
        return mask;
    }
}
