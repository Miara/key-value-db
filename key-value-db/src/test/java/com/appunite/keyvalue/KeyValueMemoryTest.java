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

import org.junit.Before;
import org.junit.Test;

import static com.google.common.truth.Truth.assert_;

public class KeyValueMemoryTest {

    public static final ByteString OBJECT1 = ByteString.copyFrom(new byte[]{123});
    public static final ByteString OBJECT2 = ByteString.copyFrom(new byte[]{124});

    private KeyValue keyValue;

    @Before
    public void setUp() throws Exception {
        keyValue = new KeyValueMemory();
    }

    @Test
    public void testAfterSettingValue_itCanBeRetrieved() throws Exception {
        keyValue.put(ByteString.copyFrom(new byte[]{0}), OBJECT1);

        assert_().that(keyValue.getBytes(ByteString.copyFrom(new byte[]{0}))).isEqualTo(OBJECT1);
    }

    @Test
    public void testGetValuesExactly_canBeRetrieved() throws Exception {
        keyValue.put(ByteString.copyFrom(new byte[]{0}), OBJECT1);

        final KeyValue.Iterator keys = keyValue.fetchValues(ByteString.copyFrom(new byte[]{0}), null, 100);

        assert_().that(keys.keys()).containsExactly(OBJECT1);
        assert_().that(keys.nextToken()).isNull();
    }

    @Test
    public void testGetValueByPrefix1_canBeRetrieved() throws Exception {
        keyValue.put(ByteString.copyFrom(new byte[]{0, 1}), OBJECT1);
        keyValue.put(ByteString.copyFrom(new byte[]{1, 1}), OBJECT2);

        final KeyValue.Iterator keys = keyValue.fetchValues(ByteString.copyFrom(new byte[]{0}), null, 100);

        assert_().that(keys.keys()).containsExactly(OBJECT1);
        assert_().that(keys.nextToken()).isNull();
    }

    @Test
    public void testGetValueByPrefix2_canBeRetrieved() throws Exception {
        keyValue.put(ByteString.copyFrom(new byte[]{0, 1}), OBJECT1);
        keyValue.put(ByteString.copyFrom(new byte[]{1, 1}), OBJECT2);

        final KeyValue.Iterator keys = keyValue.fetchValues(ByteString.copyFrom(new byte[]{1}), null, 100);

        assert_().that(keys.keys()).containsExactly(OBJECT2);
        assert_().that(keys.nextToken()).isNull();
    }

    @Test
    public void testGetValueByPrefix3_canBeRetrieved() throws Exception {
        keyValue.put(ByteString.copyFrom(new byte[]{1, 1}), OBJECT2);
        keyValue.put(ByteString.copyFrom(new byte[]{0, 1}), OBJECT1);

        final KeyValue.Iterator keys = keyValue.fetchValues(ByteString.copyFrom(new byte[]{0}), null, 100);

        assert_().that(keys.keys()).containsExactly(OBJECT1);
        assert_().that(keys.nextToken()).isNull();
    }

    @Test
    public void testIfOneValueElementIsInDbRetrieveOne_returnNullNextToken() throws Exception {
        keyValue.put(ByteString.copyFrom(new byte[]{0, 1}), OBJECT1);

        final KeyValue.Iterator keys = keyValue.fetchValues(ByteString.copyFrom(new byte[]{0}), null, 1);

        assert_().that(keys.nextToken()).isNull();
    }

    @Test
    public void testGetValueByPrefixWithMoreElements_returnThatHasMoreElements() throws Exception {
        keyValue.put(ByteString.copyFrom(new byte[]{0, 0}), OBJECT1);
        keyValue.put(ByteString.copyFrom(new byte[]{0, 1}), OBJECT2);

        final KeyValue.Iterator keys = keyValue.fetchValues(ByteString.copyFrom(new byte[]{0}), null, 1);

        assert_().that(keys.keys()).containsExactly(OBJECT1);
        assert_().that(keys.nextToken()).isNotNull();
    }

    @Test
    public void testGetSecondValueElements_returnThatHasNoMoreElements() throws Exception {
        keyValue.put(ByteString.copyFrom(new byte[]{0, 0}), OBJECT1);
        keyValue.put(ByteString.copyFrom(new byte[]{0, 1}), OBJECT2);

        final KeyValue.Iterator keys = keyValue.fetchValues(ByteString.copyFrom(new byte[]{0}), null, 1);
        final KeyValue.Iterator keys2 = keyValue.fetchValues(ByteString.copyFrom(new byte[]{0}), keys.nextToken(), 1);

        assert_().that(keys2.keys()).containsExactly(OBJECT2);
        assert_().that(keys2.nextToken()).isNull();
    }

    @Test
    public void testGetKeysExactly_canBeRetrieved() throws Exception {
        keyValue.put(ByteString.copyFrom(new byte[]{0}), OBJECT1);

        final KeyValue.Iterator keys = keyValue.fetchKeys(ByteString.copyFrom(new byte[]{0}), null, 100);

        assert_().that(keys.keys()).containsExactly(ByteString.copyFrom(new byte[]{0}));
        assert_().that(keys.nextToken()).isNull();
    }

    @Test
    public void testGetKeyByPrefix1_canBeRetrieved() throws Exception {
        keyValue.put(ByteString.copyFrom(new byte[]{0, 1}), OBJECT1);
        keyValue.put(ByteString.copyFrom(new byte[]{1, 1}), OBJECT2);

        final KeyValue.Iterator keys = keyValue.fetchKeys(ByteString.copyFrom(new byte[]{0}), null, 100);

        assert_().that(keys.keys()).containsExactly(ByteString.copyFrom(new byte[]{1, 1}));
        assert_().that(keys.nextToken()).isNull();
    }

    @Test
    public void testGetKeyByPrefix2_canBeRetrieved() throws Exception {
        keyValue.put(ByteString.copyFrom(new byte[]{0, 1}), OBJECT1);
        keyValue.put(ByteString.copyFrom(new byte[]{1, 1}), OBJECT2);

        final KeyValue.Iterator keys = keyValue.fetchKeys(ByteString.copyFrom(new byte[]{1}), null, 100);

        assert_().that(keys.keys()).containsExactly(ByteString.copyFrom(new byte[]{1, 1}));
        assert_().that(keys.nextToken()).isNull();
    }

    @Test
    public void testGetKeyByPrefix3_canBeRetrieved() throws Exception {
        keyValue.put(ByteString.copyFrom(new byte[]{1, 1}), OBJECT2);
        keyValue.put(ByteString.copyFrom(new byte[]{0, 1}), OBJECT1);

        final KeyValue.Iterator keys = keyValue.fetchKeys(ByteString.copyFrom(new byte[]{0}), null, 100);

        assert_().that(keys.keys()).containsExactly(ByteString.copyFrom(new byte[]{0, 1}));
        assert_().that(keys.nextToken()).isNull();
    }

    @Test
    public void testIfOneElementIsInDbRetrieveOne_returnNullNextToken() throws Exception {
        keyValue.put(ByteString.copyFrom(new byte[]{0, 1}), OBJECT1);

        final KeyValue.Iterator keys = keyValue.fetchKeys(ByteString.copyFrom(new byte[]{0}), null, 1);

        assert_().that(keys.nextToken()).isNull();
    }

    @Test
    public void testGetKeyByPrefixWithMoreElements_returnThatHasMoreElements() throws Exception {
        keyValue.put(ByteString.copyFrom(new byte[]{0, 0}), OBJECT1);
        keyValue.put(ByteString.copyFrom(new byte[]{0, 1}), OBJECT2);

        final KeyValue.Iterator keys = keyValue.fetchKeys(ByteString.copyFrom(new byte[]{0}), null, 1);

        assert_().that(keys.keys()).containsExactly(ByteString.copyFrom(new byte[]{0, 0}));
        assert_().that(keys.nextToken()).isNotNull();
    }

    @Test
    public void testGetSecondElements_returnThatHasNoMoreElements() throws Exception {
        keyValue.put(ByteString.copyFrom(new byte[]{0, 0}), OBJECT1);
        keyValue.put(ByteString.copyFrom(new byte[]{0, 1}), OBJECT2);

        final KeyValue.Iterator keys = keyValue.fetchKeys(ByteString.copyFrom(new byte[]{0}), null, 1);
        final KeyValue.Iterator keys2 = keyValue.fetchKeys(ByteString.copyFrom(new byte[]{0}), keys.nextToken(), 1);

        assert_().that(keys2.keys()).containsExactly(ByteString.copyFrom(new byte[]{0, 1}));
        assert_().that(keys2.nextToken()).isNull();
    }


    @Test
    public void testCanClose_withoutIssue() throws Exception {
        keyValue.close();
    }
}