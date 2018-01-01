/*
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
println("Welcome to Xodus Environment JS console. Type '/?' for help, 'exit' to exit.")

var environments = Packages.jetbrains.exodus.env;

function help() {
    println('help, /?, ?                                      - print this help');
    println('exit, quit                                       - finish session');
    println('print <smth>                                     - print something');
    println('println <smth>                                   - print something and line feed');
    println('load <path to file>                              - load script from file');
    println('open <environment location>                      - open Environment by specified location');
    println('gc                                               - invoke GC');
    println('gc [on | off]                                    - turn GC on/off');
    println('put <storeName> <key> <value> [dups] [prefixing] - put a value by key into the store');
    println('get <storeName> <key>                            - get a value by key from the store');
    println('remove <storeName> <key>                         - remove key/value from the store');
    println('Refer to Data Environment as "getEnv()", to current transaction as "txn".');
    println('Use bindings functions: stringToEntry(), entryToString(), intToEntry(), entryToInt(),');
    println('  intToCompressedEntry(), compressedEntryToInt(), longToEntry(), entryToLong(),');
    println('  longToCompressedEntry(), compressedEntryToLong(), doubleToEntry(), entryToDouble().');
}

function open(location) {
    interop.openEnvironment(location);
}

function assertEnvIsOpen() {
    if (interop.getEnv()) return true;
    println("At first, open Environment");
    return false
}

function gc(on) {
    interop.gc(on)
}

function get(storeName, key) {
    if (!assertEnvIsOpen()) return;
    var store = getEnv().openStore(storeName, environments.StoreConfig.USE_EXISTING, txn);
    return store.get(txn, key);
}

function put(storeName, key, value, duplicates, prefixing) {
    if (!assertEnvIsOpen()) return;
    var usePatriciaTree = prefixing || prefixing === undefined; // by default use Patricia tree
    var dups = duplicates !== undefined && duplicates;
    var store = getEnv().openStore(storeName, environments.StoreConfig.getStoreConfig(dups, usePatriciaTree), txn);
    return store.put(txn, key, value);
}

function remove(storeName, key) {
    if (!assertEnvIsOpen()) return;
    var store = getEnv().openStore(storeName, environments.StoreConfig.USE_EXISTING, txn);
    return store.delete(txn, key);
}