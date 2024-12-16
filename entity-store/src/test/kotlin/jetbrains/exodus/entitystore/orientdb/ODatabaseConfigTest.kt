package jetbrains.exodus.entitystore.orientdb

import org.junit.Assert
import org.junit.Test

class ODatabaseConfigTest {
    @Test
    fun `cypher key is trunked to 24 bytes from bigger one`() {
        val key1 = Array(60) { "aa" }.joinToString(separator = "")
        
        val cfg = ODatabaseConfig
            .builder()
            .withUserName("testUrl")
            .withDatabaseRoot("testPassword")
            .withStringHexAndIV(key1, 10L)
            .withDatabaseName("aa")
            .withDatabaseRoot("aa")
            .build()

        Assert.assertEquals(24, cfg.cipherKey?.size)
    }

    @Test
    fun `cypher key is not trunked if key is smaller than 24`() {
        val key1 = "aabbccddaabbccdd"

        val cfg = ODatabaseConfig
            .builder()
            .withUserName("testUrl")
            .withDatabaseRoot("testPassword")
            .withStringHexAndIV(key1, 10L)
            .withDatabaseName("aa")
            .withDatabaseRoot("aa")
            .build()

        Assert.assertEquals(16, cfg.cipherKey?.size)
    }

}
