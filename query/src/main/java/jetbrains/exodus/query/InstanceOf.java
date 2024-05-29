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
    var txn = queryEngine.getPersistentStore().getAndCheckCurrentTransaction();
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
