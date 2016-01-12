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
package jetbrains.exodus.entitystore.metadata;

import jetbrains.exodus.core.dataStructures.hash.HashSet;

import java.util.List;
import java.util.Set;

public class IndexImpl implements Index {

    private List<IndexField> fields;

    private ModelMetaData modelMetaData;

    private String ownerEnityType;

    public void setModelMetaData(ModelMetaData modelMetaData) {
        this.modelMetaData = modelMetaData;
    }

    public void setFields(List<IndexField> fields) {
        this.fields = fields;
    }

    @Override
    public List<IndexField> getFields() {
        return fields;
    }

    @Override
    public Set<String> getEntityTypesToIndex() {
        // me and inheritors
        Set<String> res = new HashSet<>();
        final String enityType = ownerEnityType;
        res.add(enityType);
        res.addAll(modelMetaData.getEntityMetaData(enityType).getAllSubTypes());
        return res;
    }

    @Override
    public String getOwnerEntityType() {
        return ownerEnityType;
    }

    public void setOwnerEnityType(String ownerEnityType) {
        this.ownerEnityType = ownerEnityType;
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

}
