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
package jetbrains.vectoriadb.index.diskcache;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.Semaphore;

import static java.util.Locale.US;

final class Node implements AccessOrderDeque.AccessOrder<Node> {
    private static final int RETIRED = -1;
    private static final int DEAD = -2;

    private volatile long key;
    private volatile long value;

    private volatile int queueType;

    @Nullable
    private volatile Node nextInAccessOrder;

    @Nullable
    private volatile Node previousInAccessOrder;

    final long pageVersion;

    final Semaphore pageLock = new Semaphore(Integer.MAX_VALUE);

    final int preLoadLocks;

    public Node(long key, long value, long pageVersion, int preLoadLocks) {
        this.key = key;
        this.value = value;
        this.pageVersion = pageVersion;
        this.preLoadLocks = preLoadLocks;

        if (preLoadLocks > 0) {
            queueType = -1;
        }
    }

    /** Return the key.*/
    public long getKey() {
        return key;
    }

    /** Return the value. */
    public long getValue() {
        return value;
    }

    /** Sets the value */
    @GuardedBy("this")
    public void setValue(long value) {
        assert value >= 0;
        this.value = value;
    }

    /* --------------- Health --------------- */

    /** If the entry is available in the hash-table and page replacement policy. */
    public boolean isAlive() {
        return key >= 0;
    }

    /** If the entry was removed from the hash-table and the page replacement policy. */
    public boolean isDead() {
        return key == DEAD;
    }

    /**
     * Sets the node to the <tt>retired</tt> state.
     * The entry was removed from the hash-table and is awaiting removal from the page
     * replacement policy.
     */
    @GuardedBy("this")
    public void retire() {
        key = RETIRED;
    }

    /** Sets the node to the <tt>dead</tt> state. */
    @GuardedBy("this")
    public void die() {
        key = DEAD;
    }

    /* --------------- Access order --------------- */

    public static final int WINDOW = 0;
    public static final int PROBATION = 1;
    public static final int PROTECTED = 2;

    /** Returns if the entry is in the Window or Main space. */
    public boolean inWindow() {
        return queueType == WINDOW;
    }

    /** Returns if the entry is in the Main space's probation queue. */
    public boolean inMainProbation() {
        return getQueueType() == PROBATION;
    }

    /** Returns if the entry is in the Main space's protected queue. */
    public boolean inMainProtected() {
        return getQueueType() == PROTECTED;
    }

    /** Sets the status to the Window queue. */
    public void makeWindow() {
        setQueueType(WINDOW);
    }

    /** Sets the status to the Main space's probation queue. */
    public void makeMainProbation() {
        setQueueType(PROBATION);
    }

    /** Sets the status to the Main space's protected queue. */
    public void makeMainProtected() {
        setQueueType(PROTECTED);
    }

    /** Returns the queue that the entry's resides in (window, probation, or protected). */
    public int getQueueType() {
        return queueType;
    }

    /** Set queue that the entry resides in (window, probation, or protected). */
    public void setQueueType(int queueType) {
        this.queueType = queueType;
    }

    @Override
    @SuppressWarnings("GuardedBy")
    public String toString() {
        return String.format(US, "%s=[key=%,d, value=%,d, queueType=%,d]", getClass().getSimpleName(),
                getKey(), value, queueType);
    }

    @Nullable
    @Override
    public Node getPreviousInAccessOrder() {
        return previousInAccessOrder;
    }

    @Override
    public void setPreviousInAccessOrder(@Nullable Node prev) {
        this.previousInAccessOrder = prev;
    }

    @Nullable
    @Override
    public Node getNextInAccessOrder() {
        return nextInAccessOrder;
    }

    @Override
    public void setNextInAccessOrder(@Nullable Node next) {
        this.nextInAccessOrder = next;
    }
}
