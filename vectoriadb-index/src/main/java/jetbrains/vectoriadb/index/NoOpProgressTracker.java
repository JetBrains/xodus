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
package jetbrains.vectoriadb.index;

public final class NoOpProgressTracker implements ProgressTracker {
    @Override
    public void start(String indexName) {
    }

    @Override
    public void pushPhase(String phaseName, String... parameters) {

    }

    @Override
    public void progress(double progress) {

    }

    @Override
    public void pullPhase() {

    }

    @Override
    public boolean isProgressUpdatedRequired() {
        return false;
    }

    @Override
    public void finish() {
    }
}
