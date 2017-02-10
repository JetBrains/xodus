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
package jetbrains.exodus.benchmark.dataStructures.persistent

import jetbrains.exodus.core.dataStructures.persistent.PersistentLong23TreeMap
import jetbrains.exodus.core.dataStructures.persistent.read
import jetbrains.exodus.core.dataStructures.persistent.writeFinally
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole
import java.util.*
import java.util.concurrent.TimeUnit

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
open class JMHPersistentLong23TreeBenchmark {

    companion object {
        const val MAP_SIZE = 100000
    }

    private val tree = PersistentLong23TreeMap<Any>()
    private val juTree = TreeMap<Long, Any>()
    private val value = Object()
    private var existingKey: Long = 0
    private var missingKey: Long = MAP_SIZE.toLong()

    @Setup
    fun prepare() {
        tree.writeFinally {
            for (i in 0..MAP_SIZE - 1) {
                // the keys are even
                put(i.toLong() * 2, value)
                juTree[i.toLong() * 2] = value
            }
        }
    }

    @Setup(Level.Invocation)
    fun prepareKeys() {
        // the even key exists in the map, the odd one doesn't
        existingKey = (Math.random() * MAP_SIZE).toLong() * 2
        missingKey = existingKey + 1
    }

    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(5)
    fun getExisting(bh: Blackhole) = bh.consume(tree.read { get(existingKey) })

    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(5)
    fun getMissing(bh: Blackhole) = bh.consume(tree.read { get(missingKey) })

    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(5)
    fun treeMapGetExisting(bh: Blackhole) = bh.consume(juTree[existingKey])

    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(5)
    fun treeMapGetMissing(bh: Blackhole) = bh.consume(juTree[missingKey])
}