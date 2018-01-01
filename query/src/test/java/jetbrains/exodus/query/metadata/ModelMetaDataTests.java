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

import jetbrains.exodus.entitystore.EntityStoreTestBase;
import org.junit.Assert;

import static jetbrains.exodus.query.metadata.MetaBuilder.clazz;
import static jetbrains.exodus.query.metadata.MetaBuilder.model;

public class ModelMetaDataTests extends EntityStoreTestBase {

    final private static String TARGET_CLASS_NAME = "TstTarget";
    final private static String SOURCE_SUPER_CLASS_NAME = "TstSuperSource";
    final private static String SOURCE_SUB_CLASS_NAME = "TstSubSource";

    public void testAddAssociationToSuperClass() throws Exception {

        ModelMetaData modelMetaData = model(
                clazz(TARGET_CLASS_NAME)
                        .prop("name", "string"),
                clazz(SOURCE_SUPER_CLASS_NAME)
                        .prop("name", "string"),
                clazz(SOURCE_SUB_CLASS_NAME, SOURCE_SUPER_CLASS_NAME)
                        .prop("number", "int")
        );

        EntityMetaData superEmd = modelMetaData.getEntityMetaData(SOURCE_SUPER_CLASS_NAME);
        EntityMetaData subEmd = modelMetaData.getEntityMetaData(SOURCE_SUB_CLASS_NAME);

        final String directLinkName = "directLink";
        modelMetaData.addAssociation(
                SOURCE_SUPER_CLASS_NAME, TARGET_CLASS_NAME, AssociationType.Directed,
                directLinkName, AssociationEndCardinality._0_1,
                false, false, false, true,
                null, null,
                false, false, false, false
                );
        Assert.assertNotNull(subEmd.getAssociationEndMetaData(directLinkName));
        Assert.assertEquals(subEmd.getAssociationEndMetaData(directLinkName), superEmd.getAssociationEndMetaData(directLinkName));

        final String forwardLinkName = "targetToSource";
        final String backwardLinkName = "sourceToTarget";
        modelMetaData.addAssociation(
                TARGET_CLASS_NAME, SOURCE_SUPER_CLASS_NAME, AssociationType.Undirected,
                forwardLinkName, AssociationEndCardinality._0_1,
                false, false, false, true,
                backwardLinkName, AssociationEndCardinality._0_1,
                false, false, false, true
        );
        Assert.assertNotNull(subEmd.getAssociationEndMetaData(backwardLinkName));
        Assert.assertEquals(subEmd.getAssociationEndMetaData(backwardLinkName), superEmd.getAssociationEndMetaData(backwardLinkName));

    }
}
