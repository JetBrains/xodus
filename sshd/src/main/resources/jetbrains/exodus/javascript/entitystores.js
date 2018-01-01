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
/**
 * Created by lvo on 1/13/2017.
 */
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

println("Welcome to Xodus EntityStores JS console. Type '/?' for help, 'exit' to exit.")

function help() {
    println('help, /?, ?                                        - print this help');
    println('exit, quit                                         - finish session');
    println('print <smth>                                       - print something');
    println('println <smth>                                     - print something and line feed');
    println('load <path to file>                                - load script from file');
    println('open <entityStore location> [entityStore name]     - open EntityStore by specified location.');
    println('all                                                - print statistics for all Entity types');
    println('all <entity type>                                  - load all entities of specified Entity type');
    println('find <entity type> <prop name> <value>             - load all entities of specified Entity type having specified property');
    println('find <entity type> <prop name> <min> <max>         - load all entities of specified Entity type having specified property in range of values');
    println('findStartingWith <entity type> <prop name> <value> - load all entities of specified Entity type having specified string property starting with value');
    println('create <entity type> [properties]                  - create an entity of specified Entity type with specified properties');
    println('entity <id>                                        - load entity by id');
    println('Refer to Entity Store as "getStore()", to current transaction as "txn".');
}

function open(location, storeName) {
    interop.openEntityStore(location, storeName);
}

function assertStoreIsOpen() {
    if (interop.getStore()) return true;
    println("At first, open Entity Store");
    return false
}

function all(type) {
    if (!assertStoreIsOpen()) return;
    if (type) {
        return txn.getAll(type);
    }
    iter(txn.getEntityTypes(), function (type) {
        println(type + ": " + txn.getAll(type).size());
    });
}

function find(type, propertyName, propertyValue, maxValue) {
    if (!assertStoreIsOpen()) return;
    if (maxValue == undefined) {
        return txn.find(type, propertyName, propertyValue);
    }
    return txn.find(type, propertyName, propertyValue, maxValue);
}

function findStartingWith(type, propertyName, propertyValue) {
    if (!assertStoreIsOpen()) return;
    return txn.findStartingWith(type, propertyName, propertyValue);
}

function create(type, props, flush) {
    if (!assertStoreIsOpen()) return;
    var entity = txn.newEntity(type);
    if (props) {
        for (var key in props) {
            if (props.hasOwnProperty(key)) {
                var val = props[key];
                entity.setProperty(key, val);
            }
        }
    }
    if (flush || flush == undefined) {
        txn.flush();
    }
    return entity
}


function entity(id) {
    if (!assertStoreIsOpen()) return;
    if (id == undefined) {
        println("Entity id is expected");
        return;
    }
    return interop.getEntity(id);
}