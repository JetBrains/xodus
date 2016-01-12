/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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

public enum StoreConfig {

    WITHOUT_DUPLICATES(0, "00000000"),
    WITH_DUPLICATES(1, "00000001"),
    WITHOUT_DUPLICATES_WITH_PREFIXING(2, "00000010"),
    WITH_DUPLICATES_WITH_PREFIXING(3, "00000011"),
    TEMPORARY_EMPTY(4, "00000100"),
    USE_EXISTING(5, "00001000");

    public final int id;

    public final boolean duplicates;
    public final boolean prefixing;
    public final boolean temporaryEmpty;
    public final boolean useExisting;

    private StoreConfig(final int id, final String mask) {
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
}
