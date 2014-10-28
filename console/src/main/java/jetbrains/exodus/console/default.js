/**
 * Created by vadim on 10/28/14.
 */
function all(type) {
  var count = 0;
  var res = "";
  var iter = txn.getAll(type).iterator();
  while (count++ < 1000 && iter.hasNext()) {
    var item = iter.next();
    res += entityToString(item) + "\n\r";
  }

  if (iter.hasNext()) {
    res += "And more...\n\r"
  }

  res += "Total " + txn.getAll(type).size();
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
