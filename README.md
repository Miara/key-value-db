# Key value db for Android

Object key-value database supporting queries. It is based on levelDB (snappyDB) but adds support for queries and object storing based on protobuf.

## Speed

Assumptions:
* transactions are lame - we don't want to worry about them
* there will be lot of inserts during cold-sync so we need add items to datbase as fast as we can (assume 10'000 messages)
* read 100 messages have to be fast as hell (first 100 messages of conversation sorted by updated at)
* there will be other messages in db ;)

### Databases under tests

* snappydb (level-db)
* sqlite
* realm
* in memory database using TreeMap


### Tests

* add - add `X` objects to database that has `2*X` in conversation before and after current conversation id (index order), (`id` - random, `created_at` - randomb)
* update - update `X` objects
* read first 100 objects - read newests posts (`created_at`) from given conversation
* read next 100 objects

### Test results

Serialization:

```
serialize 10000 - 148.4 ms
deserialize 10000 - 78.20 ms
```

Database:

```
snappy add 500: 31.59 ms
snappy update 500: 61.76 ms
snappy read first 100: 8.905 ms
snappy read second 100: 10.14 ms
sqlite add 500: 809.9 ms
sqlite update 500: 833.0 ms
sqlite read first 100: 9.741 ms
sqlite read second 100: 5.703 ms
realm add 500: 4.143 s
realm update 500: 2.341 s
realm read first 100: 10.74 ms
realm read second 100: 4.463 ms
memory add 500: 12.88 ms
memory update 500: 35.83 ms
memory read first 100: 7.175 ms
memory read second 100: 1.211 ms
snappy add 500: 24.94 ms
snappy update 500: 59.64 ms
snappy read first 200: 4.177 ms
snappy read second 200: 5.830 ms
sqlite add 500: 832.3 ms
sqlite update 500: 710.1 ms
sqlite read first 200: 7.887 ms
sqlite read second 200: 5.266 ms
realm add 500: 4.246 s
realm update 500: 2.548 s
realm read first 200: 3.387 ms
realm read second 200: 3.447 ms
memory add 500: 17.92 ms
memory update 500: 35.93 ms
memory read first 200: 1.224 ms
memory read second 200: 1.146 ms
snappy add 10000: 574.1 ms
snappy update 10000: 1.232 s
snappy read first 100: 4.795 ms
snappy read second 100: 4.732 ms
sqlite add 10000: 10.58 s
sqlite update 10000: 2.070 min
sqlite read first 100: 52.82 ms
sqlite read second 100: 48.92 ms
realm add 10000: 2.027 min
realm update 10000: 1.695 min
realm read first 100: 41.95 ms
realm read second 100: 32.95 ms
memory add 10000: 406.7 ms
memory update 10000: 760.4 ms
memory read first 100: 1.714 ms
memory read second 100: 5.580 ms
```

## How to integrate with your project

Add library to project dependencies.

```groovy
repositories {
    maven { url "https://jitpack.io" }
}

dependencies {

    // snapshot version
    compile 'com.github.jacek-marchwicki.key-value-db:key-value-db:master-SNAPSHOT'
    compile 'com.github.jacek-marchwicki.key-value-db:key-value-db-snappy-driver:master-SNAPSHOT'

    // or use specific version
    compile 'com.github.jacek-marchwicki.key-value-db:key-value-db:1.0.0'
    compile 'com.github.jacek-marchwicki.key-value-db:key-value-db-snappy-driver:1.0.0'
}
```

If you have separate project for java you can use there only `key-value-db`, and add
`key-value-db-snappy-driver` to android module.

# License

    Copyright [2015] [Jacek Marchwicki <jacek.marchwicki@gmail.com>]
    
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    
    	http://www.apache.org/licenses/LICENSE-2.0
        
    
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
