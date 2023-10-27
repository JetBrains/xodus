/*
 * Copyright ${inceptionYear} - ${year} ${owner}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.vectoriadb.server;

import jetbrains.vectoriadb.index.AbstractPeriodicProgressTracker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ListenerBasedPeriodicProgressTracker extends AbstractPeriodicProgressTracker {
    private final Set<IndexBuildProgressListener> listeners = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public ListenerBasedPeriodicProgressTracker(int period) {
        super(period);
    }

    @Override
    protected void reportProgress() {
        if (listeners.isEmpty()) {
            return;
        }

        if (this.phases.isEmpty()) {
            return;
        }

        String indexName = null;
        ArrayList<AbstractPeriodicProgressTracker.ProgressPhase> phases = new ArrayList<>();
        phaseLoop:
        while (true) {
            for (var phase : this.phases) {
                if (indexName == null) {
                    indexName = phase.indexName;
                } else if (!indexName.equals(phase.indexName)) {
                    continue phaseLoop;
                }

                phases.add(phase);
            }
            break;
        }

        var progressPhases = new IndexBuildProgressListener.IndexBuildProgressPhase[phases.size()];

        for (int i = 0; i < phases.size(); i++) {
            var phase = phases.get(i);
            progressPhases[i] = new IndexBuildProgressListener.IndexBuildProgressPhase(phase.phaseName,
                    phase.progress, phase.parameters);
        }

        var progressInfo = new IndexBuildProgressListener.IndexBuildProgressInfo(indexName, progressPhases);
        for (var listener: listeners) {
            listener.progress(progressInfo);
        }
    }

    public void addListener(IndexBuildProgressListener listener) {
        listeners.add(listener);
    }

    public void removeListener(IndexBuildProgressListener listener) {
        listeners.remove(listener);
    }
}
