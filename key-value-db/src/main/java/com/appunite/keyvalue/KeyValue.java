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

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.appunite.keyvalue.internal.Preconditions.checkNotNull;

public interface KeyValue extends EditOperations {

    interface Batch extends EditOperations {
        void clear();
        void write();
    }

    class FakeBatch implements Batch {

        @Nonnull
        private final KeyValue keyValue;
        @Nonnull
        private final List<Operation> operations = new ArrayList<>();

        public FakeBatch(@Nonnull KeyValue keyValue) {
            this.keyValue = keyValue;
        }

        @Override
        public void put(@Nonnull ByteString key, @Nonnull ByteString value) {
            checkNotNull(key);
            checkNotNull(value);
            operations.add(new PutOperation(key, value));
        }

        @Override
        public void del(@Nonnull ByteString key) {
            checkNotNull(key);
            operations.add(new DelOperation(key));
        }

        @Override
        public void clear() {
            operations.clear();
        }

        @Override
        public void write() {
            for (Operation operation : operations) {
                operation.apply(keyValue);
            }
        }

        interface Operation {
            void apply(@Nonnull KeyValue keyValue);
        }
        private static class DelOperation implements Operation {
            @Nonnull
            private final ByteString key;

            DelOperation(@Nonnull ByteString key) {
                this.key = key;
                checkNotNull(key);
            }

            @Override
            public void apply(@Nonnull KeyValue keyValue) {
                keyValue.del(key);
            }
        }
        private static class PutOperation implements Operation {

            @Nonnull
            private final ByteString key;
            @Nonnull
            private final ByteString value;

            private PutOperation(@Nonnull ByteString key, @Nonnull ByteString value) {
                this.key = key;
                this.value = value;
                checkNotNull(key);
                checkNotNull(value);
            }

            @Override
            public void apply(@Nonnull KeyValue keyValue) {
                keyValue.put(key, value);
            }
        }
    }

    @Nonnull
    Batch newBatch();

    @Nonnull
    ByteString getBytes(@Nonnull ByteString key) throws NotFoundException;

    /**
     * You should use {@link #fetchValues(ByteString, ByteString, int)} instead
     */
    @Nonnull
    @Deprecated
    Iterator getKeys(@Nonnull ByteString prefix,
                     @Nullable ByteString nextTokenOrNull,
                     int batch);

    @Nonnull
    Iterator fetchValues(@Nonnull ByteString prefix,
                         @Nullable ByteString nextTokenOrNull,
                         int batch);

    @Nonnull
    Iterator fetchKeys(@Nonnull ByteString prefix,
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