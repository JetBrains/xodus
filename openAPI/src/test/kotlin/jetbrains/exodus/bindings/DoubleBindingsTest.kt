/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
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
package jetbrains.exodus.bindings

import org.junit.Assert
import org.junit.Test

class DoubleBindingsTest {

    @Test
    fun values() {
        (((0L..1000L) + Long.MAX_VALUE).map { it.toDouble() } + Double.MAX_VALUE).forEach { d ->
            Assert.assertTrue(d == DoubleBinding.entryToDouble(DoubleBinding.doubleToEntry(d)))
        }
        (((-1000L..1000L) + Long.MIN_VALUE + Long.MAX_VALUE).map { it.toDouble() } + Double.MIN_VALUE + Double.MAX_VALUE).forEach { d ->
            Assert.assertTrue(d == SignedDoubleBinding.entryToDouble(SignedDoubleBinding.doubleToEntry(d)))
        }
    }

    @Test
    fun order() {
        Assert.assertTrue(DoubleBinding.doubleToEntry(.0) < DoubleBinding.doubleToEntry(1.0))
        Assert.assertTrue(DoubleBinding.doubleToEntry(1.0) < DoubleBinding.doubleToEntry(256.0))
        Assert.assertTrue(DoubleBinding.doubleToEntry(256.0) < DoubleBinding.doubleToEntry(65536.0))
        Assert.assertTrue(DoubleBinding.doubleToEntry(65536.0) < DoubleBinding.doubleToEntry(Double.MAX_VALUE))

        Assert.assertTrue(SignedDoubleBinding.doubleToEntry(.0) < SignedDoubleBinding.doubleToEntry(1.0))
        Assert.assertTrue(SignedDoubleBinding.doubleToEntry(1.0) < SignedDoubleBinding.doubleToEntry(256.0))
        Assert.assertTrue(SignedDoubleBinding.doubleToEntry(256.0) < SignedDoubleBinding.doubleToEntry(65536.0))
        Assert.assertTrue(SignedDoubleBinding.doubleToEntry(65536.0) < SignedDoubleBinding.doubleToEntry(Double.MAX_VALUE))
        Assert.assertTrue(SignedDoubleBinding.doubleToEntry(.0) > SignedDoubleBinding.doubleToEntry(-1.0))
        Assert.assertTrue(SignedDoubleBinding.doubleToEntry(-1.0) > SignedDoubleBinding.doubleToEntry(-256.0))
        Assert.assertTrue(SignedDoubleBinding.doubleToEntry(-256.0) > SignedDoubleBinding.doubleToEntry(-65536.0))
        Assert.assertTrue(SignedDoubleBinding.doubleToEntry(-65536.0) > SignedDoubleBinding.doubleToEntry(-Double.MAX_VALUE))
    }
}