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

package jetbrains.exodus.benchmark.util;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class JMHByteBufferBVarHandleDirectIndex {
    private static final ByteBuffer buffer = ByteBuffer.allocate(1);
    private static final byte[] array = new byte[1];
    private static final VarHandle BUFFER_HANDLE = MethodHandles.byteBufferViewVarHandle(byte.class,
            ByteOrder.nativeOrder());

    @Benchmark
    public byte getBuffer() {
        return buffer.get(0);
    }

    @Benchmark
    public byte indexArray() {
        return array[0];
    }

    public byte handleGet() {
        return (byte) BUFFER_HANDLE.get(0, buffer);
    }

    @Benchmark
    public byte baseLine() {
        return 0;
    }

}
