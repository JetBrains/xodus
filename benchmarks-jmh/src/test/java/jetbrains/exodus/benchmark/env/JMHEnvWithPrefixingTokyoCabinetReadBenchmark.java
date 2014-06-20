package jetbrains.exodus.benchmark.env;

import jetbrains.exodus.env.StoreConfig;

public class JMHEnvWithPrefixingTokyoCabinetReadBenchmark extends JMHEnvTokyoCabinetReadBenchmark {

    @Override
    protected StoreConfig getConfig() {
        return StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING;
    }
}
