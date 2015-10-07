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

import junit.framework.TestCase;

import java.nio.ByteBuffer;

import static com.google.common.truth.Truth.assert_;

public class ByteUtilsTest extends TestCase {

    public void testConvertToStringAndBack1_isEquals() throws Exception {
        final ByteString bytes = ByteString.copyFrom(new byte[]{'a', 'b', 'c'});

        final ByteString out = ByteUtils.fromString(ByteUtils.toString(bytes));

        assert_().that(out).isEqualTo(bytes);
    }

    public void testConvertToStringAndBack2_isEquals() throws Exception {
        final ByteString bytes = ByteString.copyFrom(new byte[]{'\0', 'O', (byte) 255, (byte) -1});

        final ByteString out = ByteUtils.fromString(ByteUtils.toString(bytes));

        assert_().that(out).isEqualTo(bytes);
    }

    public void testConvertToBytes_isEqualsToBuff() throws Exception {
        byte[] bytes = {0, 1, 2};
        final ByteBuffer buffer = ByteBuffer.allocate(3);
        buffer.put(bytes);

        byte[] out = ByteUtils.convertToBytes(buffer);

        assert_().that(out).isEqualTo(bytes);
    }

}