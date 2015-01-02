/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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
package jetbrains.exodus.core.dataStructures;

public abstract class CacheHitRateable {

    private static final short ADAPTIVE_ATTEMPTS_THRESHOLD = 30000;

    private short attempts;
    private short hits;

    protected CacheHitRateable() {
        attempts = hits = 0;
    }

    public final short getAttempts() {
        return attempts;
    }

    public final short getHits() {
        return hits;
    }

    public final float hitRate() {
        return attempts > 0 ? (float) hits / (float) attempts : 0;
    }

    protected final void incAttempts() {
        if (++attempts > ADAPTIVE_ATTEMPTS_THRESHOLD) {
            adjustHitRate();
        }
    }

    protected final void incHits() {
        ++hits;
    }

    protected void adjustHitRate() {
        attempts >>= 1;
        hits >>= 1;
    }
}
