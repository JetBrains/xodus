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

import org.openjdk.jmh.annotations.*;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class JMHPutIntByteBufferVarHandle {
    private static final ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.nativeOrder());

    private static final VarHandle BUFFER_HANDLE = MethodHandles.byteBufferViewVarHandle(int[].class,
            ByteOrder.nativeOrder());

    private static final byte[] array = new byte[Integer.BYTES];
    private static final VarHandle ARRAY_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class,
            ByteOrder.nativeOrder());

    private int value;

    @Benchmark
    public void putBuffer() {
        buffer.putInt(0, 42);
    }

    @Benchmark
    public void putVarHandle() {
        BUFFER_HANDLE.set(buffer, 0, 42);
    }

    @Benchmark
    public void putArrayVarHandle() {
        ARRAY_HANDLE.set(array, 0, 42);
    }

    @Benchmark
    public void baseLine() {
    }

    @Benchmark
    public void setValue() {
        this.value = 42;
    }
}
