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
package jetbrains.vectoriadb.index;

import org.jetbrains.annotations.NotNull;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

class ByteCodeSegment implements CodeSegment {

    @NotNull
    private final MemorySegment codes;

    ByteCodeSegment(@NotNull MemorySegment codes) {
        this.codes = codes;
    }

    @Override
    public int count() {
        return (int) codes.byteSize();
    }

    @Override
    public int get(int vectorIdx) {
        return codes.get(ValueLayout.JAVA_BYTE, vectorIdx) & 0xFF;
    }

    @Override
    public void set(int vectorIdx, int value) {
        codes.setAtIndex(ValueLayout.JAVA_BYTE, vectorIdx, (byte) value);
    }

    @Override
    public int maxNumberOfCodes() {
        return 256;
    }

    public static CodeSegment makeNativeSegment(Arena arena, int count) {
        var memorySegment = arena.allocate(count, ValueLayout.JAVA_BYTE.byteAlignment());
        return new ByteCodeSegment(memorySegment);
    }

    public static CodeSegment makeArraySegment(int count) {
        var segment = MemorySegment.ofArray(new byte[count]);
        return new ByteCodeSegment(segment);
    }

    /**
     * Allocates codes the way that, for a given vector, the codes are close to each other in the memory.
     * So physically, the memory layout looks like this
     * vector1_code1, vector1_code2, vector2_code1, vector2_code2...
     */
    public static CodeSegment[] makeNativeSegments(Arena arena, int codeCount, int vectorCount) {
        var memorySegment = arena.allocate((long) codeCount * vectorCount, ValueLayout.JAVA_BYTE.byteAlignment());
        return makeNativeSegments(memorySegment, codeCount);
    }

    public static CodeSegment[] makeNativeSegments(MemorySegment segment, int codeCount) {
        var result = new CodeSegment[codeCount];
        for (int i = 0; i < codeCount; i++) {
            result[i] = new SkippingByteCodeSegment(segment, codeCount, i);
        }
        return result;
    }
}
