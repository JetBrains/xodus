package jetbrains.exodus.benchmark.je;

public class JMH_JEWithPrefixingTokyoCabinetLikeWriteBenchmark extends JMH_JETokyoCabinetLikeWriteBenchmark {

    @Override
    protected boolean isKeyPrefixing() {
        return true;
    }
}
