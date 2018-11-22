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
    @TestFor(issues = ["XD-761"])
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
