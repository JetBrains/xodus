package jetbrains.vectoriadb.index;

public final class ConsolePeriodicProgressTracker extends AbstractPeriodicProgressTracker {
    public ConsolePeriodicProgressTracker(int period) {
        super(period);
    }

    @Override
    void printProgress() {
        var status = createConsoleOutput();

        if (!status.isBlank()) {
            System.out.println(status);
        }
    }
}
