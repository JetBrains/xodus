/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.vfs;

import jetbrains.exodus.env.Environment;
import org.jetbrains.annotations.NotNull;

/**
 * {@code VfsConfig} is used to configure creation of {@linkplain VirtualFileSystem}. It defines which type of
 * {@linkplain ClusteringStrategy} to use and whether the {@linkplain VirtualFileSystem} should accumulate all changes
 * in RAM before flushing or committing transactions. If transactions are too heavy to fit changes in RAM, then
 * {@linkplain VirtualFileSystem} uses temporary files on underlying storage device.
 *
 * @see VirtualFileSystem#VirtualFileSystem(Environment, VfsConfig)
 * @see ClusteringStrategy
 */
public class VfsConfig {

    public static final VfsConfig DEFAULT = new VfsConfig();

    @NotNull
    private ClusteringStrategy strategy;
    private boolean accumulateChangesInRAM;

    public VfsConfig() {
        strategy = ClusteringStrategy.QUADRATIC;
        accumulateChangesInRAM = true;
    }

    /**
     * Returns {@linkplain ClusteringStrategy} used by the {@linkplain VirtualFileSystem} to save contents of a file.
     * Default value used by the {@code VfsConfig} is {@linkplain ClusteringStrategy#QUADRATIC}
     *
     * @return {@linkplain ClusteringStrategy} used by the {@linkplain VirtualFileSystem} to save contents of a file
     */
    @NotNull
    public ClusteringStrategy getClusteringStrategy() {
        return strategy;
    }

    /**
     * Sets {@linkplain ClusteringStrategy} used by the {@linkplain VirtualFileSystem} to save contents of a file.
     *
     * @param strategy {@linkplain ClusteringStrategy} instance
     */
    public void setClusteringStrategy(@NotNull final ClusteringStrategy strategy) {
        this.strategy = strategy;
    }

    /**
     * @return {@code true} if the {@linkplain VirtualFileSystem} should accumulate all changes in RAM before flushing
     * or committing transactions
     */
    public boolean getAccumulateChangesInRAM() {
        return accumulateChangesInRAM;
    }

    public void setAccumulateChangesInRAM(final boolean accumulateChangesInRAM) {
        this.accumulateChangesInRAM = accumulateChangesInRAM;
    }
}
