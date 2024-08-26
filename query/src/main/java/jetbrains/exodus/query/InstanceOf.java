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
package jetbrains.exodus.query;

import jetbrains.exodus.entitystore.Entity;
import jetbrains.exodus.entitystore.orientdb.iterate.property.OInstanceOfIterable;
import jetbrains.exodus.query.metadata.ModelMetaData;

public class InstanceOf extends NodeBase {

  private final String className;
  private final Boolean invert;

  public InstanceOf(String className, Boolean invert) {
    this.className = className;
    this.invert = invert;
  }

  @Override
  public Iterable<Entity> instantiate(String entityType, QueryEngine queryEngine, ModelMetaData metaData, InstantiateContext context) {
    var txn = queryEngine.getOStore().requireActiveTransaction();
    return new OInstanceOfIterable(txn, entityType, className, invert);
  }

  @Override
  public NodeBase getClone() {
    return new InstanceOf(className, invert);
  }

  @Override
  public String getSimpleName() {
    if (invert){
      return "NOT InstanceOf " + className;
    } else  {
      return "InstanceOf " + className;
    }
  }
}
