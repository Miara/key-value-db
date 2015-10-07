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
import java.util.Arrays;

import javax.annotation.Nonnull;

public class ByteUtils {

    private static final char[] HEX_CHARS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A',
            'B', 'C', 'D', 'E', 'F' };

    @Nonnull
    public static String toString(@Nonnull ByteString bytes) {
        final int size = bytes.size();
        char[] hexChars = new char[size * 2];
        int v;
        for (int j = 0; j < size; j++) {
            v = bytes.byteAt(j) & 0xFF;
            hexChars[j * 2] = HEX_CHARS[v >>> 4];
            hexChars[j * 2 + 1] = HEX_CHARS[v & 0x0F];
        }
        return new String(hexChars);
    }

    @Nonnull
    public static ByteString fromString(@Nonnull String fromString) {
        int size = fromString.length() / 2;
        byte[] out = new byte[size];

        for (int i = 0; i < size; ++i) {
            final byte c1 = toByte(fromString.charAt(i * 2));
            final byte c2 = toByte(fromString.charAt(i*2+1));
            out[i] = (byte)(c2 + (c1 << 4));
        }
        return ByteString.copyFrom(out);
    }

    private static byte toByte(char c) {
        if (c >= 'A') {
            return (byte)(c - 'A' + 10);
        } else {
            return (byte)(c - '0');
        }
    }

    @Nonnull
    public static byte[] convertToBytes(@Nonnull ByteBuffer buffer) {
        final byte[] array = buffer.array();
        final int arrayOffset = buffer.arrayOffset();
        return Arrays.copyOfRange(array, arrayOffset,
                arrayOffset + buffer.position());
    }

    @Nonnull
    public static ByteString convertToByteString(@Nonnull ByteBuffer buffer) {
        final byte[] array = buffer.array();
        final int arrayOffset = buffer.arrayOffset();

        return ByteString.copyFrom(array, arrayOffset, arrayOffset + buffer.position());
    }

}
