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

package com.appunite.keyvalue;

import com.google.protobuf.ByteString;

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface KeyValue {

    void put(@Nonnull ByteString key, @Nonnull ByteString value);

    void del(@Nonnull ByteString key);

    @Nonnull
    ByteString getBytes(@Nonnull ByteString key) throws NotFoundException;

    @Nonnull
    Iterator getKeys(@Nonnull ByteString prefix,
                     @Nullable ByteString nextTokenOrNull,
                     int batch);

    void close();

    class Iterator {
        @Nonnull
        private final List<ByteString> keys;
        @Nullable
        private final ByteString nextToken;

        public Iterator(@Nonnull List<ByteString> keys,
                        @Nullable ByteString nextToken) {
            this.keys = keys;
            this.nextToken = nextToken;
        }

        @Nonnull
        public List<ByteString> keys() {
            return keys;
        }

        @Nullable
        public ByteString nextToken() {
            return nextToken;
        }
    }
}
