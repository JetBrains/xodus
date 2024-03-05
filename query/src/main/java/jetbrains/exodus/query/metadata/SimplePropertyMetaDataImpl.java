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
package jetbrains.exodus.query.metadata;

import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.List;

/**
 */
public class SimplePropertyMetaDataImpl extends PropertyMetaDataImpl {

    private String primitiveTypeName;

    private List<String> typeParameterNames;

    public SimplePropertyMetaDataImpl() {
    }

    public SimplePropertyMetaDataImpl(final String name, final String primitiveTypeName) {
        this(name, primitiveTypeName, Collections.emptyList());
    }

    public SimplePropertyMetaDataImpl(final String name, final String primitiveTypeName, final List<String> typeParameterNames) {
        super(name, PropertyType.PRIMITIVE);
        this.primitiveTypeName = primitiveTypeName;
        this.typeParameterNames = typeParameterNames;
    }

    @Nullable
    public String getPrimitiveTypeName() {
        return primitiveTypeName;
    }

    /**
     * If you have a property of type Set[String], String is the type parameter.
     * So, getPrimitiveTypeName() returns "Set" and getTypeParameterNames() returns ["String"].
     * */
    public List<String> getTypeParameterNames() { return typeParameterNames; }

    public void setPrimitiveTypeName(String primitiveTypeName) {
        this.primitiveTypeName = primitiveTypeName;
    }
}
