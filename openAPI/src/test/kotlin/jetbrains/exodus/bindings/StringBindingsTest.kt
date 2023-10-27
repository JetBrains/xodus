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
package jetbrains.exodus.bindings

import jetbrains.exodus.TestFor
import jetbrains.exodus.util.LightOutputStream
import org.junit.Assert
import org.junit.Test
import java.nio.charset.StandardCharsets

class StringBindingsTest {
    companion object {
        val hash = byteArrayOf(-17, -65, -67, 68, 85, 96, -17, -65, -67, 28, 4, 76, 28, -17, -65, -67, -17, -65, -67, 90, 17, -17, -65, -67, 0, 95)
        val serializedHash = byteArrayOf(-17, -65, -67, 68, 85, 96, -17, -65, -67, 28, 4, 76, 28, -17, -65, -67, -17, -65, -67, 90, 17, -17, -65, -67, -64, -128, 95, 0)

        val stringType = ComparableValueType.getPredefinedType(String::class.java).typeId
        val stringBinding = ComparableValueType.getPredefinedBinding(stringType)
    }

    @Test()
    @TestFor(issue = "XD-761")
    fun testWeirdChars() {
        val string = String(hash, StandardCharsets.UTF_8)
        val value = propertyValueToEntry(string)

        Assert.assertArrayEquals(serializedHash, value)
    }

    private fun propertyValueToEntry(value: String): ByteArray {
        val output = LightOutputStream()
        stringBinding.writeObject(output, value)
        return output.bufferBytes.copyOf(output.size())
    }
}
