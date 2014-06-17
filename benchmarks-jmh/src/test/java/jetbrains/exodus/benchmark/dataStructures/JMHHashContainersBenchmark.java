/**
 * Copyright 2010 - 2014 JetBrains s.r.o.
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
package jetbrains.exodus.benchmark.dataStructures;

import jetbrains.exodus.core.dataStructures.hash.HashMap;
import org.openjdk.jmh.annotations.*;

import java.util.Map;

@SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
@State(Scope.Thread)
public class JMHHashContainersBenchmark {

    final Map<Integer, String> javaUtilMap = new HashMap<Integer, String>();
    int j = 0;

    @Setup
    public void prepare() {
        for (int i = 0; i < 100000; ++i) {
            javaUtilMap.put(i, Integer.toString(i));
        }
    }

    @Setup(Level.Invocation)
    public void changeIndex() {
        j++;
        if (j >= 100000) {
            j = 0;
        }
    }

    @Benchmark
    public String javaUtilHashMapGet() {
        return javaUtilMap.get(j);
    }

/*
    public void benchmarkHashMapGet() {

        long started;

        final Map<Integer, String> map = new java.util.HashMap<Integer, String>();
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                map.get(j);
            }
        }
        System.out.println("100 000 000 lookups in java.util.HashMap took " + (System.currentTimeMillis() - started));

        final HashMap<Integer, String> tested = new HashMap<Integer, String>();
        for (int i = 0; i < 100000; ++i) {
            tested.put(i, Integer.toString(i));
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                tested.get(j);
            }
        }
        System.out.println("100 000 000 lookups in HashMap took " + (System.currentTimeMillis() - started));
    }
*/

}
