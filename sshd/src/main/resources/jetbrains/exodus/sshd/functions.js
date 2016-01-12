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

    var getSize = entityIterable.size;
    if (getSize) {
        println("Total " + getSize());
    }
}

function printEntity(item) {
    println(item.getType() + " " + item.getId());

    iter(item.getPropertyNames(), function(property) {
        println(property + "=[" + item.getProperty(property) + "]");
    });

    iter(item.getLinkNames(), function(link) {
        println(link + "=[" + item.getLink(link) + "]");
    });
}

function stat() {
    iter(txn.getEntityTypes(), function(type) {
        println(type + ": " + txn.getAll(type).size());
    });
}

function all(type) {
    printEntityIterable(txn.getAll(type));
}

function find(type, propertyName, propertyValue, maxValue) {
    if (maxValue == undefined) {
        printEntityIterable(txn.find(type, propertyName, propertyValue));
    } else {
        printEntityIterable(txn.find(type, propertyName, propertyValue, maxValue));
    }
}

function findStartingWith(type, propertyName, propertyValue) {
    printEntityIterable(txn.findStartingWith(type, propertyName, propertyValue));
}

function iter(iterable, f) {
    var iter = iterable.iterator();
    while (iter.hasNext()) {
        var item = iter.next();
        f(item);
    }
}

function gc(on) {
    var cfg = env.getEnvironmentConfig();

    if (on !== undefined) {
        cfg.setGcEnabled(on);
    }

    println('GC is ' + (cfg.isGcEnabled() === true ? 'on' : 'off'));
}

function add(type, props, flush, print) {
    var entity = txn.newEntity(type);

    if (props) {
        for(var key in props){
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
        printEntity(entity);
    }
}
