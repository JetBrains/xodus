package jetbrains.exodus.benchmark.env;

import jetbrains.exodus.env.StoreConfig;

public class JMHEnvWithPrefixingTokyoCabinetWriteBenchmark extends JMHEnvTokyoCabinetWriteBenchmark {

    @Override
    protected StoreConfig getConfig() {
        return StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING;
    }
}
