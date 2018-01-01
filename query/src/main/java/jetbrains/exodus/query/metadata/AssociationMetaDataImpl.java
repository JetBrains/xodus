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

import java.util.ArrayList;
import java.util.List;

public class AssociationMetaDataImpl implements AssociationMetaData {

    private AssociationType type;
    private final List<AssociationEndMetaData> ends = new ArrayList<>(2);
    private String fullName;

    public AssociationMetaDataImpl() {
    }

    public AssociationMetaDataImpl(AssociationType type, String fullName) {
        this.type = type;
        this.fullName = fullName;
    }

    public String getFullName() {
        return fullName;
    }

    public void setFullName(final String fullName) {
        this.fullName = fullName;
    }

    public void addEnd(@NotNull AssociationEndMetaData end) {
        if (ends.isEmpty() || (ends.size() < 2 && ends.get(0) != end)) {
            ends.add(end);
        }
    }

    public void setType(@NotNull AssociationType type) {
        this.type = type;
    }

    @Override
    @NotNull
    public AssociationType getType() {
        return type;
    }

    @Override
    @NotNull
    public AssociationEndMetaData getOppositeEnd(@NotNull AssociationEndMetaData end) {
        if (AssociationType.Directed.equals(type)) {
            // directed association
            throw new IllegalStateException("Directed association has no opposite end.");
        }

        if (ends.size() != 1 && ends.size() != 2) {
            throw new IllegalStateException("Incomplete association.");
        }

        return end == ends.get(0) ? ends.get(1) : ends.get(0);
    }
}
