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
package jetbrains.exodus.query.metadata;

import org.jetbrains.annotations.NotNull;

@SuppressWarnings("EnumeratedConstantNamingConvention")
public enum AssociationEndCardinality {

    _0_1("0..1"),
    _1("1"),
    _0_n("0..n"),
    _1_n("1..n");

    private final String name;

    AssociationEndCardinality(String name) {
        this.name = name;
    }

    @NotNull
    public String getName() {
        return name;
    }

    public boolean isMultiple() {
        return this == _0_n || this == _1_n;
    }

}
