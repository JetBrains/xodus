var MAX_ITEMS_TO_PRINT = 1000;

function all(type) {
  return entityIterableToString(txn.getAll(type));
}

function find(type, propertyName, propertyValue) {
  return entityIterableToString(txn.find(type, propertyName, propertyValue));
}

function entityIterableToString(entityIterable) {
  var iter = entityIterable.iterator();
  var count = 0;
  var res = "";
  while (count++ < MAX_ITEMS_TO_PRINT && iter.hasNext()) {
    var item = iter.next();
    res += entityToString(item) + "\n\r";
  }

  if (iter.hasNext()) {
    res += "And more...\n\r"
  }

  res += "Total " + entityIterable.size();
  return res;
}

function entityToString(item) {
  var res = item.getType() + " " + item.getId() + "\n\r";

  var properties = item.getPropertyNames().iterator();
  while (properties.hasNext()) {
    var property = properties.next();
    res += property + "=[" + item.getProperty(property) + "]\n\r"
  }

  return res;
}
