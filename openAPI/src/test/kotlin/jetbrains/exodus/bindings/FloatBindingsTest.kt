/*
 * Copyright 2010 - 2024 JetBrains s.r.o.
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

import org.junit.Assert.assertTrue
import org.junit.Test

class FloatBindingsTest {

    @Test
    fun values() {
        (((0..1000) + Int.MAX_VALUE).map { it.toFloat() } + Float.MAX_VALUE).forEach { f ->
            assertTrue(f == FloatBinding.entryToFloat(FloatBinding.floatToEntry(f)))
        }
        (((-1000..1000) + Int.MIN_VALUE + Int.MAX_VALUE).map { it.toFloat() } + Float.MIN_VALUE + Float.MAX_VALUE).forEach { f ->
            assertTrue(f == SignedFloatBinding.entryToFloat(SignedFloatBinding.floatToEntry(f)))
        }
    }

    @Test
    fun order() {
        assertTrue(FloatBinding.floatToEntry(0f) < FloatBinding.floatToEntry(1f))
        assertTrue(FloatBinding.floatToEntry(1f) < FloatBinding.floatToEntry(256f))
        assertTrue(FloatBinding.floatToEntry(256f) < FloatBinding.floatToEntry(65536f))
        assertTrue(FloatBinding.floatToEntry(65536f) < FloatBinding.floatToEntry(Float.MAX_VALUE))

        assertTrue(SignedFloatBinding.floatToEntry(0f) < SignedFloatBinding.floatToEntry(1f))
        assertTrue(SignedFloatBinding.floatToEntry(1f) < SignedFloatBinding.floatToEntry(256f))
        assertTrue(SignedFloatBinding.floatToEntry(256f) < SignedFloatBinding.floatToEntry(65536f))
        assertTrue(SignedFloatBinding.floatToEntry(65536f) < SignedFloatBinding.floatToEntry(Float.MAX_VALUE))
        assertTrue(SignedFloatBinding.floatToEntry(0f) > SignedFloatBinding.floatToEntry(-1f))
        assertTrue(SignedFloatBinding.floatToEntry(-1f) > SignedFloatBinding.floatToEntry(-256f))
        assertTrue(SignedFloatBinding.floatToEntry(-256f) > SignedFloatBinding.floatToEntry(-65536f))
        assertTrue(SignedFloatBinding.floatToEntry(-65536f) > SignedFloatBinding.floatToEntry(-Float.MAX_VALUE))
    }
}