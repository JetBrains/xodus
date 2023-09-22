package jetbrains.vectoriadb.index;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractPeriodicProgressTracker implements ProgressTracker {
    private final Timer timer = new Timer("ProgressTracker", true);
    private final long period;
    private volatile boolean timerStarted = false;
    private final ReentrantLock timerLock = new ReentrantLock();
    private final AtomicBoolean requestProgressUpdate = new AtomicBoolean(true);
    final ArrayList<ProgressPhase> phases = new ArrayList<>();

    public AbstractPeriodicProgressTracker(int period) {
        this.period = TimeUnit.SECONDS.toMillis(period);
    }

    @Override
    public void pushPhase(String phaseName, String... parameters) {
        var phase = new ProgressPhase(phaseName, parameters);
        phases.add(phase);
        printProgress();
    }

    @Override
    public void progress(double progress) {
        scheduleProgressTimer();

        var phase = phases.get(phases.size() - 1);
        phase.progress = Math.max(phase.progress, Math.min(100, Math.max(0, progress)));

        if (requestProgressUpdate.get()) {
            requestProgressUpdate.compareAndSet(true, false);
            printProgress();
        }
    }

    abstract void printProgress();

    @NotNull
    String createConsoleOutput() {
        if (phases.isEmpty()) {
            return "";
        }

        StringBuilder builder = new StringBuilder();
        builder.append("Progress status: ");
        for (int i = 0; i < phases.size(); i++) {
            var phase = phases.get(i);
            builder.append(phase.phaseName);
            var parameters = phase.parameters;

            if (parameters.length > 0) {
                builder.append(" ");
            }

            for (int j = 0; j < parameters.length; j += 2) {
                builder.append("{");
                builder.append(parameters[j]);
                builder.append(":");
                builder.append(parameters[j + 1]);
                builder.append("}");

                if (j < parameters.length - 2) {
                    builder.append(", ");
                }
            }
            if (phase.progress >= 0) {
                builder.append(" [").append(String.format("%.2f", phase.progress)).append("%]");
            }

            if (phase != phases.get(phases.size() - 1)) {
                builder.append(" -> ");
            }
        }
        return builder.toString();
    }

    private void scheduleProgressTimer() {
        if (!timerStarted) {
            timerLock.lock();
            try {
                if (timerStarted) {
                    return;
                }

                timer.scheduleAtFixedRate(new TimerTask() {
                    @Override
                    public void run() {
                        requestProgressUpdate.set(true);
                    }
                }, period, period);

                timerStarted = true;
            } finally {
                timerLock.unlock();
            }
        }
    }

    @Override
    public void pullPhase() {
        phases.remove(phases.size() - 1);
    }

    @Override
    public boolean isProgressUpdatedRequired() {
        return requestProgressUpdate.get();
    }

    static final class ProgressPhase {
        double progress = -1;
        final String phaseName;
        final String[] parameters;

        public ProgressPhase(String phaseName, String... parameters) {
            this.phaseName = phaseName;
            this.parameters = parameters;
        }
    }
}
