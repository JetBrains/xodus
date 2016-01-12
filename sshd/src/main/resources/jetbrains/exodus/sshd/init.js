/*
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
var MAX_ITEMS_TO_PRINT = 1000;

function print(s) {
  if (!s) s = "";
  api.print(s);
}

function println(s) {
  if (!s) s = "";
  api.println(s);
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
