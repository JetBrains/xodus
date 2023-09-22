package jetbrains.vectoriadb.index;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Slf4jPeriodicProgressTracker extends AbstractPeriodicProgressTracker {
    private static final Logger logger = LoggerFactory.getLogger(Slf4jPeriodicProgressTracker.class);

    public Slf4jPeriodicProgressTracker(int period) {
        super(period);
    }

    @Override
    void printProgress() {
        var status = createConsoleOutput();
        if (!status.isBlank()) {
            logger.info(status);
        }
    }
}
