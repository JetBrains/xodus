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

interop.println("Welcome to Xodus EntityStores JS console. Type '/?' for help, 'exit' to exit.")

function help() {
    interop.println('help(), help, /?, ?                                - print this help.');
    interop.println('exit, quit                                         - finish terminal session.');
    interop.println('load <path to file>                                - load script from file');
    interop.println('open <entityStore location> [entityStore name]     - open EntityStore by specified location.');
    interop.println('all                                                - print statistics for all Entity types');
    interop.println('all <entity type>                                  - print all entities of specified Entity type');
    interop.println('find <entity type> <prop name> <value>             - print all entities of specified Entity type having specified property');
    interop.println('find <entity type> <prop name> <min> <max>         - print all entities of specified Entity type having specified property in range of values');
    interop.println('findStartingWith <entity type> <prop name> <value> - print all entities of specified Entity type having specified string property starting with value');
    interop.println('create <entity type> [properties]                  - create an entity of specified Entity type with specified properties');
}

function open(location, storeName) {
    interop.openEntityStore(location, storeName);
}

function all(type) {
    if (type) {
        interop.print(txn.getAll(type));
    } else {
        iter(txn.getEntityTypes(), function (type) {
            interop.println(type + ": " + txn.getAll(type).size());
        });
    }
}

function find(type, propertyName, propertyValue, maxValue) {
    if (maxValue == undefined) {
        interop.print(txn.find(type, propertyName, propertyValue));
    } else {
        interop.print(txn.find(type, propertyName, propertyValue, maxValue));
    }
}

function findStartingWith(type, propertyName, propertyValue) {
    interop.print(txn.findStartingWith(type, propertyName, propertyValue));
}

function create(type, props, flush, print) {
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
    if (print || print == undefined) {
        interop.print(entity);
    }
}