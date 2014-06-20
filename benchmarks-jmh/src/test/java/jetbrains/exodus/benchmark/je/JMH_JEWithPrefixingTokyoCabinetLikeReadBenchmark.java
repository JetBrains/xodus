package jetbrains.exodus.benchmark.je;

public class JMH_JEWithPrefixingTokyoCabinetLikeReadBenchmark extends JMH_JETokyoCabinetLikeReadBenchmark {

    @Override
    protected boolean isKeyPrefixing() {
        return true;
    }
}
