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

package com.appunite.keyvalue.driver.level;

import android.app.Instrumentation;
import android.content.Context;

import androidx.test.ext.junit.runners.AndroidJUnit4;
import androidx.test.filters.MediumTest;
import androidx.test.platform.app.InstrumentationRegistry;

import com.appunite.keyvalue.KeyValue;
import com.google.protobuf.ByteString;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.Locale;
import java.util.Random;

import static com.google.common.truth.Truth.assert_;

@RunWith(AndroidJUnit4.class)
@MediumTest
public class KeyValueLevelTest {

    public static final ByteString OBJECT1 = ByteString.copyFrom(new byte[]{123});
    public static final ByteString OBJECT2 = ByteString.copyFrom(new byte[]{124});

    private KeyValue keyValue;

    @Before
    public void setUp() throws Exception {
        final Context targetContext = InstrumentationRegistry.getInstrumentation().getTargetContext();
        final String format = String.format(Locale.US, "Db:%d.db", new Random().nextLong());
        final File databasePath = targetContext.getDatabasePath(format);
        deleteRecursive(databasePath);
        keyValue = KeyValueLevel.create(targetContext, databasePath);
    }

    @After
    public void tearDown() throws Exception {
        keyValue.close();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    void deleteRecursive(File fileOrDirectory) {
        if (fileOrDirectory.isDirectory()) {
            for (File child : fileOrDirectory.listFiles()) {
                deleteRecursive(child);
            }
        }
        fileOrDirectory.delete();
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
    public void testIfOneElementIsInDbRetrieveOne_returnNullNextToken() throws Exception {
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
    public void testGetSecondElements_returnThatHasNoMoreElements() throws Exception {
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

        assert_().that(keys.keys()).containsExactly(ByteString.copyFrom(new byte[]{0, 1}));
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
    public void testGetKeyByPrefixWithMoreElements_returnThatHasMoreElements() throws Exception {
        keyValue.put(ByteString.copyFrom(new byte[]{0, 0}), OBJECT1);
        keyValue.put(ByteString.copyFrom(new byte[]{0, 1}), OBJECT2);

        final KeyValue.Iterator keys = keyValue.fetchKeys(ByteString.copyFrom(new byte[]{0}), null, 1);

        assert_().that(keys.keys()).containsExactly(ByteString.copyFrom(new byte[]{0, 0}));
        assert_().that(keys.nextToken()).isNotNull();
    }

    @Test
    public void testGetSecondKeyElements_returnThatHasNoMoreElements() throws Exception {
        keyValue.put(ByteString.copyFrom(new byte[]{0, 0}), OBJECT1);
        keyValue.put(ByteString.copyFrom(new byte[]{0, 1}), OBJECT2);

        final KeyValue.Iterator keys = keyValue.fetchKeys(ByteString.copyFrom(new byte[]{0}), null, 1);
        final KeyValue.Iterator keys2 = keyValue.fetchKeys(ByteString.copyFrom(new byte[]{0}), keys.nextToken(), 1);

        assert_().that(keys2.keys()).containsExactly(ByteString.copyFrom(new byte[]{0, 1}));
        assert_().that(keys2.nextToken()).isNull();
    }
}