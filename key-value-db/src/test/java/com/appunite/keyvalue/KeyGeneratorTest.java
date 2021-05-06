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

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

import org.junit.Before;
import org.junit.Test;

import static com.google.common.truth.Truth.assert_;

public class KeyGeneratorTest {

    private KeyGenerator generator;

    @Before
    public void setUp() throws Exception {
        generator = new KeyGenerator();
    }

    @Test
    public void testTwoIndexesWithDifferentInt_hasSameSize() throws Exception {
        final ByteString message1 = generator.startIndex("messages".getBytes())
                .addField("local_id".getBytes(), ByteString.copyFromUtf8("localId1"))
                .addField("created_at".getBytes(), 1L)
                .buildQuery();

        for (Long value : ImmutableList.of(10L, 100L, 210L, 1000L, 10000L, 100000L, 10000000L)) {
            final ByteString message2 = generator.startIndex("messages".getBytes())
                    .addField("local_id".getBytes(), ByteString.copyFromUtf8("localId1"))
                    .addField("created_at".getBytes(), value)
                    .buildQuery();
            assert_().that(message1.size()).isEqualTo(message2.size());
        }
    }

    @Test
    public void testGraterValue_hasGraterIndex() throws Exception {
        final ByteString message1 = generator.startIndex("messages".getBytes())
                .addField("local_id".getBytes(), ByteString.copyFromUtf8("localId1"))
                .addField("created_at".getBytes(), 1L)
                .buildQuery();

        for (Long value : ImmutableList.of(2L, 10L, 100L, 210L, 1000L, 10000L, 100000L, 10000000L)) {
            final ByteString message2 = generator.startIndex("messages".getBytes())
                    .addField("local_id".getBytes(), ByteString.copyFromUtf8("localId1"))
                    .addField("created_at".getBytes(), value)
                    .buildQuery();
            final int compared = KeyValueMemory.COMPARATOR.compare(message1, message2);
            assert_().that(compared).isLessThan(0);
        }
    }

    @Test
    public void testSmallerValue_hasSmallerIndex() throws Exception {
        final ByteString message1 = generator.startIndex("messages".getBytes())
                .addField("local_id".getBytes(), ByteString.copyFromUtf8("localId1"))
                .addField("created_at".getBytes(), 10000001L)
                .buildQuery();

        for (Long value : ImmutableList.of(2L, 10L, 100L, 210L, 1000L, 10000L, 100000L, 10000000L)) {
            final ByteString message2 = generator.startIndex("messages".getBytes())
                    .addField("local_id".getBytes(), ByteString.copyFromUtf8("localId1"))
                    .addField("created_at".getBytes(), value)
                    .buildQuery();
            final int compared = KeyValueMemory.COMPARATOR.compare(message1, message2);
            assert_().that(compared).isGreaterThan(0);
        }
    }

    @Test
    public void testTwoIndexes_hasEqualValue() throws Exception {
        final ByteString message1 = generator.startIndex("messages".getBytes())
                .addField("created_at".getBytes(), 1)
                .buildQuery();
        final ByteString message2 = generator.startIndex("messages".getBytes())
                .addField("created_at".getBytes(), 1)
                .buildQuery();

        assert_().that(KeyValueMemory.COMPARATOR.compare(message1, message2)).isEqualTo(0);
    }

}