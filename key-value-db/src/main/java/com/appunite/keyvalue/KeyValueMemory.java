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

import com.appunite.keyvalue.internal.Preconditions;
import com.appunite.keyvalue.internal.UnsignedBytes;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

import javax.annotation.Nonnull;
import javax.inject.Inject;

import static com.appunite.keyvalue.internal.Preconditions.checkNotNull;

public class KeyValueMemory implements KeyValue {
    public static final Comparator<ByteString> COMPARATOR = new Comparator<ByteString>() {
        @Override
        public int compare(ByteString o1, ByteString o2) {
            final int size1 = o1.size();
            final int size2 = o2.size();
            final int max = Math.min(size1, size2);
            for (int i = 0; i < max; i++) {
                final byte b1 = o1.byteAt(i);
                final byte b2 = o2.byteAt(i);
                final int compare = UnsignedBytes.compare(b1, b2);
                if (compare != 0) {
                    return compare;
                }
            }
            if (size1 > size2) {
                return 1;
            } else if (size1 < size2) {
                return -1;
            }
            return 0;
        }

    };
    private final TreeMap<ByteString, ByteString> map = new TreeMap<>(COMPARATOR);

    @Inject
    public KeyValueMemory() {
    }

    @Override
    public void put(@Nonnull ByteString key, @Nonnull ByteString value) {
        checkNotNull(key);
        checkNotNull(value);
        map.put(key, value);
    }

    @Override
    public void del(@Nonnull ByteString key) {
        checkNotNull(key);
        map.remove(key);
    }

    @Nonnull
    @Override
    public ByteString getBytes(@Nonnull ByteString key) throws NotFoundException {
        checkNotNull(key);
        final ByteString bytes = map.get(key);
        if (bytes == null) {
            throw new NotFoundException();
        }
        return bytes;
    }

    @Nonnull
    @Override
    public Iterator getKeys(@Nonnull ByteString prefix, ByteString nextTokenOrNull, int batch) {
        checkNotNull(prefix);
        Preconditions.checkArgument(batch >= 1);
        final ArrayList<ByteString> values = new ArrayList<>();

        Map.Entry<ByteString, ByteString> entry = map.ceilingEntry(nextTokenOrNull == null ? prefix : nextTokenOrNull);
        for (;;) {
            if (entry == null) {
                return new Iterator(values, null);
            }
            final ByteString key = entry.getKey();
            if (!key.startsWith(prefix)) {
                return new Iterator(values, null);
            }
            if (values.size() == batch) {
                return new Iterator(values, key);
            }
            values.add(entry.getValue());


            entry = map.higherEntry(key);
        }
    }

    @Override
    public void close() {
    }
}
