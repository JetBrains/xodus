/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
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
package jetbrains.exodus.query.metadata;

import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.concurrent.Callable;

public class IndexImpl implements Index {

    private List<IndexField> fields;

    private String ownerEntityType;

    private Callable<String> errorMessageBuilder;

    public void setFields(List<IndexField> fields) {
        this.fields = fields;
    }

    @Override
    public List<IndexField> getFields() {
        return fields;
    }

    @Override
    public String getOwnerEntityType() {
        return ownerEntityType;
    }

    public void setOwnerEntityType(String ownerEntityType) {
        this.ownerEntityType = ownerEntityType;
    }

    @Deprecated
    public void setOwnerEnityType(String ownerEntityType) {
        setOwnerEntityType(ownerEntityType);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        boolean first = true;
        for (IndexField f : fields) {
            if (!first) {
                sb.append(", ");
            } else {
                first = false;
            }
            sb.append(f.toString());
        }

        return sb.toString();
    }

    @Nullable
    @Override
    public Callable<String> getErrorMessageBuilder() {
        return errorMessageBuilder;
    }

    public void setErrorMessageBuilder(Callable<String> errorMessageBuilder) {
        this.errorMessageBuilder = errorMessageBuilder;
    }
}
