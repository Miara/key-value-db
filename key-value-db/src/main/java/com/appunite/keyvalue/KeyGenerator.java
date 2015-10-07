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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import javax.annotation.Nonnull;

public class KeyGenerator {
    @Nonnull
    private final Builder builder = new Builder();
    @Nonnull
    private final ByteBuffer conversationIndex = ByteBuffer.allocate(200);

    private static final byte[] INDEX = "index".getBytes();
    private static final byte[] VALUE = "value".getBytes();
    private static final byte SEPARATOR = (byte) 6;

    public KeyGenerator() {
        conversationIndex.order(ByteOrder.BIG_ENDIAN);
    }

    @Nonnull
    public Builder startIndex(@Nonnull byte[] valueType) {
        conversationIndex.clear();
        conversationIndex.put(INDEX);
        conversationIndex.put(SEPARATOR);
        conversationIndex.put(valueType);
        conversationIndex.put(SEPARATOR);
        return builder;
    }

    @Nonnull
    public ByteString singleValue(@Nonnull byte[] valueType) {
        conversationIndex.clear();
        conversationIndex.put(VALUE);
        conversationIndex.put(SEPARATOR);
        conversationIndex.put(valueType);
        conversationIndex.put(SEPARATOR);
        return ByteUtils.convertToByteString(conversationIndex);
    }

    @Nonnull
    public ByteString value(@Nonnull byte[] valueType, @Nonnull ByteString id) {
        conversationIndex.clear();
        conversationIndex.put(VALUE);
        conversationIndex.put(SEPARATOR);
        conversationIndex.put(valueType);
        conversationIndex.put(SEPARATOR);
        conversationIndex.put(id.toByteArray());
        conversationIndex.put(SEPARATOR);
        return ByteUtils.convertToByteString(conversationIndex);
    }

    public class Builder {
        private Builder() {
        }

        @Nonnull
        public ByteString buildQuery() {
            return ByteUtils.convertToByteString(conversationIndex);
        }

        @Nonnull
        public ByteString buildIndex(@Nonnull ByteString id) {
            conversationIndex.put(id.toByteArray());
            conversationIndex.put(SEPARATOR);
            return ByteUtils.convertToByteString(conversationIndex);
        }

        @Nonnull
        public Builder addField(@Nonnull byte[] fieldName, @Nonnull ByteString value) {
            conversationIndex.put(fieldName);
            conversationIndex.put(SEPARATOR);
            conversationIndex.put(value.toByteArray());
            conversationIndex.put(SEPARATOR);
            return this;
        }

        @Nonnull
        public Builder addField(@Nonnull byte[] fieldName, long value) {
            conversationIndex.put(fieldName);
            conversationIndex.put(SEPARATOR);
            conversationIndex.putLong(value);
            conversationIndex.put(SEPARATOR);
            return this;
        }

        @Nonnull
        public Builder addFieldReverted(@Nonnull byte[] fieldName, long value) {
            conversationIndex.put(fieldName);
            conversationIndex.put(SEPARATOR);
            conversationIndex.putLong(Long.MAX_VALUE - value);
            conversationIndex.put(SEPARATOR);
            return this;
        }

        @Nonnull
        public Builder addField(@Nonnull byte[] fieldName, boolean value) {
            conversationIndex.put(fieldName);
            conversationIndex.put(SEPARATOR);
            conversationIndex.putInt(value ? 0 : 1);
            conversationIndex.put(SEPARATOR);
            return this;
        }
    }
}
