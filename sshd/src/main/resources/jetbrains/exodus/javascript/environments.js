/*
 * Copyright 2010 - 2017 JetBrains s.r.o.
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

interop.println("Welcome to Xodus Environment JS console. Type '/?' for help, 'exit' to exit.")

var environments = Packages.jetbrains.exodus.env;

function help() {
    interop.println('help(), help, /?, ?                              - print this help');
    interop.println('exit, quit                                       - finish terminal session');
    interop.println('load <path to file>                              - load script from file');
    interop.println('open <environment location>                      - open Environment by specified location');
    interop.println('gc                                               - invoke GC');
    interop.println('gc [on | off]                                    - turn GC on/off');
    interop.println('put <storeName> <key> <value> [dups] [prefixing] - put a value by key into the store');
    interop.println('get <storeName> <key>                            - get a value by key from the store');
    interop.println('remove <storeName> <key>                         - remove key/value from the store');
}

function open(location) {
    interop.openEnvironment(location);
}

function gc(on) {
    interop.gc(on)
}

function get(storeName, key) {
    var store = interop.getEnv().openStore(storeName, environments.StoreConfig.USE_EXISTING, txn);
    return store.get(txn, key);
}

function put(storeName, key, value, duplicates, prefixing) {
    var usePatriciaTree = prefixing || prefixing === undefined; // by default use Patricia tree
    var dups = duplicates !== undefined && duplicates;
    var store = interop.getEnv().openStore(storeName, environments.StoreConfig.getStoreConfig(dups, usePatriciaTree), txn);
    return store.put(txn, key, value);
}

function remove(storeName, key) {
    var store = interop.getEnv().openStore(storeName, environments.StoreConfig.USE_EXISTING, txn);
    return store.delete(txn, key);
}