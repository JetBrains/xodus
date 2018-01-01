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

import jetbrains.exodus.core.dataStructures.hash.HashSet;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static jetbrains.exodus.query.metadata.AssociationEndCardinality._0_1;
import static jetbrains.exodus.query.metadata.AssociationEndType.DirectedAssociationEnd;
import static jetbrains.exodus.query.metadata.AssociationEndType.UndirectedAssociationEnd;
import static jetbrains.exodus.query.metadata.AssociationType.Directed;
import static jetbrains.exodus.query.metadata.AssociationType.Undirected;

@SuppressWarnings("rawtypes")
public class MetaBuilder {

    private MetaBuilder() {
    }

    public static ModelMetaDataImpl model() {
        return new ModelMetaDataImpl();
    }

    public static ModelMetaDataImpl model(final Enumeration... classes) {
        return model(model(), classes);
    }

    public static ModelMetaDataImpl model(final ModelMetaDataImpl modelMetaData, final Enumeration... classes) {
        final Set<EntityMetaData> emdSet = new HashSet<>();
        for (final Enumeration enumeration : classes) {
            emdSet.add(enumeration.build(modelMetaData));
        }
        modelMetaData.setEntityMetaDatas(emdSet);
        modelMetaData.init();
        return modelMetaData;
    }

    public static Clazz clazz(final Class clz) {
        return clazz(clz.getName());
    }

    public static Clazz clazz(final String name) {
        return new Clazz(name);
    }

    public static Clazz clazz(final Class clz, final Class superClz) {
        return new Clazz(clz.getName(), superClz.getName());
    }

    public static Clazz clazz(final String name, final String superType) {
        return new Clazz(name, superType);
    }

    public static Enumeration enumeration(final Class clz) {
        return enumeration(clz.getName());
    }

    public static Enumeration enumeration(final String name) {
        return new Enumeration(name);
    }

    public static void main(final String... args) {
        model(
                clazz("TstClass").
                        prop("s", "string").
                        prop("i", "int").
                        link("itself", "TstClass", _0_1).
                        edge("self1", "TstClass", _0_1, "self2", _0_1).
                        link("myEnum", "MyEnum", _0_1),
                clazz("TstClassInheritor", "TstClass"),
                enumeration("MyEnum").
                        prop("number", "int")
        );
    }

    @SuppressWarnings("ChainOfInstanceofChecks")
    public static class Enumeration {

        protected final String type;
        protected final Set<MemberMetaData> meta = new HashSet<>();
        protected final Set<AssociationMetaData> assoc = new HashSet<>();

        public Enumeration(String type) {
            this.type = type;
        }

        public Enumeration prop(final String name, final String type) {
            meta.add(new SimplePropertyMetaDataImpl(name, type));
            return this;
        }

        public Enumeration text(final String name) {
            meta.add(new PropertyMetaDataImpl(name, PropertyType.TEXT));
            return this;
        }

        public Enumeration blob(final String name) {
            meta.add(new PropertyMetaDataImpl(name, PropertyType.BLOB));
            return this;
        }

        public EntityMetaDataImpl build(final ModelMetaDataImpl model) {
            final EntityMetaDataImpl result = new EntityMetaDataImpl();
            result.setType(type);
            final List<PropertyMetaData> p = new LinkedList<>();
            final List<AssociationEndMetaData> a = new LinkedList<>();
            for (final MemberMetaData member : meta) {
                if (member instanceof PropertyMetaData) {
                    p.add((PropertyMetaData) member);
                } else if (member instanceof AssociationEndMetaData) {
                    a.add((AssociationEndMetaData) member);
                } else {
                    throw new UnsupportedOperationException("unknown member type");
                }
            }
            model.setAssociationMetaDatas(assoc);
            result.setPropertiesMetaData(p);
            result.setAssociationEndsMetaData(a);
            return result;
        }
    }

    public static final class Clazz extends Enumeration {
        private String superType;

        public Clazz(String name) {
            super(name);
        }

        public Clazz(String name, String superType) {
            super(name);
            this.superType = superType;
        }

        public Clazz link(final String name, final String toType, AssociationEndCardinality cardinality) {
            final AssociationEndMetaDataImpl result = new AssociationEndMetaDataImpl();
            result.setName(name);
            result.setOppositeEntityMetaDataType(toType);
            result.setAssociationEndType(DirectedAssociationEnd);
            result.setCardinality(cardinality);
            final String assocName = type + '.' + name + '-' + toType;
            final AssociationMetaDataImpl amd = new AssociationMetaDataImpl(Directed, assocName);
            result.setAssociationMetaDataName(assocName);
            result.setAssociationMetaDataInternal(amd);
            assoc.add(amd);
            meta.add(result);
            return this;
        }

        public Clazz edge(final String name, final String toType, final AssociationEndCardinality cardinality,
                          final String toName, final AssociationEndCardinality toCardinality) {
            final AssociationEndMetaDataImpl from = new AssociationEndMetaDataImpl();
            from.setName(name);
            from.setOppositeEntityMetaDataType(toType);
            from.setAssociationEndType(UndirectedAssociationEnd);
            from.setCardinality(cardinality);
            from.setOppositeEndName(toName);
            final AssociationEndMetaDataImpl to = new AssociationEndMetaDataImpl();
            to.setName(toName);
            to.setOppositeEntityMetaDataType(type);
            to.setAssociationEndType(UndirectedAssociationEnd);
            to.setCardinality(toCardinality);
            to.setOppositeEndName(name);
            final String assocName = type + '.' + name + '-' + toType + '.' + toName;
            final AssociationMetaDataImpl amd = new AssociationMetaDataImpl(Undirected, assocName);
            from.setAssociationMetaDataName(assocName);
            to.setAssociationMetaDataName(assocName);
            from.setAssociationMetaDataInternal(amd);
            to.setAssociationMetaDataInternal(amd);
            assoc.add(amd);
            meta.add(from);
            meta.add(to);
            return this;
        }

        @Override
        public Clazz prop(String name, String type) {
            super.prop(name, type);
            return this;
        }

        @Override
        public Clazz text(String name) {
            super.text(name);
            return this;
        }

        @Override
        public Clazz blob(String name) {
            super.blob(name);
            return this;
        }

        @Override
        public EntityMetaDataImpl build(ModelMetaDataImpl model) {
            final EntityMetaDataImpl result = super.build(model);
            if (superType != null) {
                result.setSuperType(superType);
            }
            return result;
        }
    }

}
