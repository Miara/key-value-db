/*
 * Copyright 2015 Jacek Marchwicki <jacek.marchwicki@gmail.com>
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

package com.appunite.example.snappy;

import android.content.Context;

import com.appunite.keyvalue.KeyValueMemory;
import com.appunite.keyvalue.driver.level.KeyValueLevel;
import com.appunite.keyvalue.driver.snappy.KeyValueSnappy;

public class DatabaseProvider {

    public static Database provide(Context context, int databaseType, String name) throws Exception {
        switch (databaseType) {
            case 0:
                return new DatabaseSnappy(KeyValueSnappy.create(context, name));
            case 1:
                return new DatabaseSql(context, name);
            case 2:
                return new DatabaseRealm(context, name);
            case 3:
                return new DatabaseSnappy(new KeyValueMemory());
            case 4: {
                return new DatabaseSnappy(KeyValueLevel.create(context, context.getDatabasePath(name)));
            }
            default:
                throw new RuntimeException("Unknown database type: " + databaseType);
        }
    }
}
