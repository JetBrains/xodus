var MAX_ITEMS_TO_PRINT = 1000;

function print(s) {
  if (!s) s = "";
  out.print(s);
  out.flush();
}

function println(s) {
  if (!s) s = "";
  out.print(s + "\n\r");
  out.flush();
}

function newEnvironment(location) {
  var e = Packages.jetbrains.exodus.env;
  return e.Environments.newInstance(location, e.EnvironmentConfig());
}

function newStore(environment, name) {
  return Packages.jetbrains.exodus.entitystore.PersistentEntityStores.newInstance(environment, name ? name : 'teamsysstore');
}

println(config.implementationVersion);
if (config.store) {
  store = config.store;
  env = store.getEnvironment();
} else if (config.location) {
  env = newEnvironment(config.location);
  println('Created environemnt on ' + env.getLocation());
  store = newStore(env);
}

println('Welcome to Xodus console. To exit press Ctrl-C.');
