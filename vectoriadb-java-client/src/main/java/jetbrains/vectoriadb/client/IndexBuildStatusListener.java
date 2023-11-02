package jetbrains.vectoriadb.client;

import java.util.List;

public interface IndexBuildStatusListener {
    boolean onIndexBuildStatusUpdate(String indexName, List<Phase> phases);

    record Phase(String name, double progress, String... parameters) {
    }
}
