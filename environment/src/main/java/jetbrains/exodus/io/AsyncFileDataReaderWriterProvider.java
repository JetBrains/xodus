package jetbrains.exodus.io;

import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.EnvironmentImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class AsyncFileDataReaderWriterProvider extends DataReaderWriterProvider {
    private @Nullable EnvironmentImpl env;

    @Override
    public Pair<DataReader, DataWriter> newReaderWriter(@NotNull String location) {
        var reader = new FileDataReader(WatchingFileDataReaderWriterProvider.checkDirectory(location));
        String lockId = null;

        if (env != null) {
            lockId = env.getEnvironmentConfig().getLogLockId();
        }

        return new Pair<>(reader, new AsyncFileDataWriter(reader, lockId));
    }

    @Override
    public void onEnvironmentCreated(@NotNull Environment environment) {
        this.env = (EnvironmentImpl) environment;
        super.onEnvironmentCreated(environment);
    }
}
