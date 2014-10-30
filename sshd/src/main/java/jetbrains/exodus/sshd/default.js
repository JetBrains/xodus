var MAX_ITEMS_TO_PRINT = 1000;

function print(s) {
  out.print(s);
  out.flush();
}

function println(s) {
  if (!s) s = "";
  out.print(s + "\n\r");
  out.flush();
}

function all(type) {
  return printEntityIterable(txn.getAll(type));
}

function find(type, propertyName, propertyValue) {
  return printEntityIterable(txn.find(type, propertyName, propertyValue));
}

function printEntityIterable(entityIterable) {
  var iter = entityIterable.iterator();
  var count = 0;
  while (count++ < MAX_ITEMS_TO_PRINT && iter.hasNext()) {
    var item = iter.next();
    printEntity(item);
    println();
  }

  if (iter.hasNext()) {
    println("And more...");
  }

  println("Total " + entityIterable.size());
}

function printEntity(item) {
  println(item.getType() + " " + item.getId());

  var properties = item.getPropertyNames().iterator();
  while (properties.hasNext()) {
    var property = properties.next();
    println(property + "=[" + item.getProperty(property) + "]");
  }

  var links = item.getLinkNames().iterator();
  while (links.hasNext()) {
    var link = links.next();
    println(link + "=[" + item.getLink(link) + "]");
  }
}
