package jetbrains.exodus.entitystore

import com.orientechnologies.orient.core.db.ODatabaseType
import jetbrains.exodus.entitystore.orientdb.ODatabaseConfig
import jetbrains.exodus.entitystore.orientdb.initOrientDbServer
import java.nio.file.Files
import kotlin.io.path.absolutePathString
import kotlin.test.Test

class WrongUsernameTest {

    @Test(expected = IllegalArgumentException::class)
    fun cannotCreateDatabaseWithWrongUsername() {
        val cfg = ODatabaseConfig
            .builder()
            .withUserName(";drop database users")
            .withDatabaseType(ODatabaseType.MEMORY)
            .withDatabaseName("hello")
            .withDatabaseRoot(Files.createTempDirectory("haha").absolutePathString())
            .build()
        initOrientDbServer(cfg)
    }


}
