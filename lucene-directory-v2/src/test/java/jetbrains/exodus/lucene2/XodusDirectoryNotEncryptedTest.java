package jetbrains.exodus.lucene2;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.EnvironmentConfig;
import jetbrains.exodus.env.Environments;
import org.apache.lucene.store.BaseDirectoryTestCase;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.TestRuleLimitSysouts;

import java.io.IOException;
import java.nio.file.Path;

@ThreadLeakFilters(filters = XodusThreadFilter.class)
@TestRuleLimitSysouts.Limit(bytes = 22 * 1024)
public class XodusDirectoryNotEncryptedTest extends BaseDirectoryTestCase {
    @Override
    protected Directory getDirectory(Path path) throws IOException {
        final EnvironmentConfig config = new EnvironmentConfig();
        config.setLogCachePageSize(1024);
        config.removeSetting(EnvironmentConfig.CIPHER_ID);
        config.removeSetting(EnvironmentConfig.CIPHER_KEY);

        Environment environment = Environments.newInstance(path.toFile(), config);

        return new XodusDirectory(environment);
    }
}
