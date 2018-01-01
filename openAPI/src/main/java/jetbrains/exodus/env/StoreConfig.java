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
package jetbrains.exodus.env;

/**
 * Specifies the attributes of a {@linkplain Store}. There are two attributes that can vary independently:
 * {@code duplicates} and {@code prefixing}. If {@code duplicates} is {@code true}, then the {@linkplain Store}
 * can save several values for a single key. If {@code prefixing} is {@code true}, then the {@linkplain Store} uses
 * <a href="https://en.wikipedia.org/wiki/Radix_tree">Patricia trie</a> as a search tree, rather than
 * <a href="https://en.wikipedia.org/wiki/B%2B_tree">B+Tree</a>. Patricia tries tend to perform better and save
 * physical space of the database if the keys tend to have common prefixes. Also these types of search tree differ in
 * performance characteristics: stores with key prefixing has better random key access, whereas stores without key
 * prefixing are preferable for sequential access in order of keys.
 *
 * <table border=1>
 * <tr><td/><th>With duplicates</th><th>Without duplicates</th></tr>
 * <tr><th>With key prefixing</th><td>{@link #WITH_DUPLICATES_WITH_PREFIXING}</td><td>{@link #WITH_DUPLICATES_WITH_PREFIXING}</td></tr>
 * <tr><th>Without key prefixing</th><td>{@link #WITH_DUPLICATES}</td><td>{@link #WITHOUT_DUPLICATES}</td></tr>
 * </table>
 */
public enum StoreConfig {

    /**
     * Store with this config has no key duplicates and doesn't support key prefixing.
     */
    WITHOUT_DUPLICATES(0, "00000000"),
    /**
     * Store with this config has key duplicates and doesn't support key prefixing.
     */
    WITH_DUPLICATES(1, "00000001"),
    /**
     * Store with this config has no key duplicates and supports key prefixing.
     */
    WITHOUT_DUPLICATES_WITH_PREFIXING(2, "00000010"),
    /**
     * Store with this config has no key duplicates and supports key prefixing.
     */
    WITH_DUPLICATES_WITH_PREFIXING(3, "00000011"),
    /**
     * Please don't use it in your applications.
     */
    TEMPORARY_EMPTY(4, "00000100"),
    /**
     * If it is known that a {@linkplain Store} definitely exists it can be opened with the
     * {@code StoreConfig.USE_EXISTING} configuration. In that case, you don't need to know whether the
     * {@linkplain Store} can have duplicate keys or was it created with key prefixing.
     */
    USE_EXISTING(5, "00001000");

    public final int id;

    public final boolean duplicates;
    public final boolean prefixing;
    public final boolean temporaryEmpty;
    public final boolean useExisting;

    StoreConfig(final int id, final String mask) {
        this.id = id;
        final int bits = Integer.parseInt(mask, 2);
        duplicates = (bits & 1) > 0;
        prefixing = ((bits >> 1) & 1) > 0;
        temporaryEmpty = ((bits >> 2) & 1) > 0;
        useExisting = ((bits >> 3) & 1) > 0;
    }

    @Override
    public String toString() {
        return "duplicates: " + duplicates + ", prefixing: " + prefixing + ", temporaryEmpty: " + temporaryEmpty + ", useExisting: " + useExisting;
    }

    /**
     * Returns {@code StoreConfig} value corresponding to the specified {@linkplain Store} attributes.
     *
     * <table border=1>
     * <tr><td/><th>With duplicates</th><th>Without duplicates</th></tr>
     * <tr><th>With key prefixing</th><td>{@link #WITH_DUPLICATES_WITH_PREFIXING}</td><td>{@link #WITH_DUPLICATES_WITH_PREFIXING}</td></tr>
     * <tr><th>Without key prefixing</th><td>{@link #WITH_DUPLICATES}</td><td>{@link #WITHOUT_DUPLICATES}</td></tr>
     * </table>
     *
     * @param duplicates {@code true} if key duplicates are allowed
     * @param prefixing  {@code true} if key prefixing is desired
     * @return {@code StoreConfig} value corresponding to the specified {@linkplain Store} attributes
     */
    public static StoreConfig getStoreConfig(final boolean duplicates, final boolean prefixing) {
        if (duplicates) {
            return prefixing ? WITH_DUPLICATES_WITH_PREFIXING : WITH_DUPLICATES;
        }
        return prefixing ? WITHOUT_DUPLICATES_WITH_PREFIXING : WITHOUT_DUPLICATES;
    }
}
