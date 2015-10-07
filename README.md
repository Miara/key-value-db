# Key value db for Android
[![build status](http://gitlab-ci.appunite.net/projects/42/status.png?ref=master)](http://gitlab-ci.appunite.net/projects/42?ref=master)

Key value database supporting queries based on key-value snappy db

# How to integrate with your project
In your poject directory

```bash
git submodule add <repo> key-value-db
```

add to settings your settings gradle:

```groovy
include ":key-value-db"
include ":key-value-db-snappy-driver"

project(':key-value-db').projectDir = new File('key-value-db/key-value-db')
project(':key-value-db-snappy-driver').projectDir = new File('key-value-db/key-value-db-snappy-driver')
```

In your api project `build.gradle`:

```groovy
compile project(":key-value-db")
```

In your android project `build.gradle`:

```bash
compile project(":key-value-db-snappy-driver")
```

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
