/*
 * Copyright (C) 2009 The Guava Authors
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

package com.appunite.keyvalue.internal;

public class UnsignedBytes {
    /**
     * Compares the two specified {@code byte} values, treating them as unsigned
     * values between 0 and 255 inclusive. For example, {@code (byte) -127} is
     * considered greater than {@code (byte) 127} because it is seen as having
     * the value of positive {@code 129}.
     *
     * @param a the first {@code byte} to compare
     * @param b the second {@code byte} to compare
     * @return a negative value if {@code a} is less than {@code b}; a positive
     *     value if {@code a} is greater than {@code b}; or zero if they are equal
     */
    public static int compare(byte a, byte b) {
        return toInt(a) - toInt(b);
    }

    private static final int UNSIGNED_MASK = 0xFF;

    /**
     * Returns the value of the given byte as an integer, when treated as
     * unsigned. That is, returns {@code value + 256} if {@code value} is
     * negative; {@code value} itself otherwise.
     *
     * @since 6.0
     */
    public static int toInt(byte value) {
        return value & UNSIGNED_MASK;
    }
}
