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

package com.appunite.keyvalue.driver.snappy;

import android.content.Context;

import com.appunite.keyvalue.ByteUtils;
import com.appunite.keyvalue.KeyValue;
import com.appunite.keyvalue.NotFoundException;
import com.appunite.keyvalue.driver.snappy.internal.Preconditions;
import com.google.protobuf.ByteString;
import com.snappydb.DB;
import com.snappydb.KeyIterator;
import com.snappydb.SnappyDB;
import com.snappydb.SnappydbException;

import java.util.ArrayList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class KeyValueSnappy implements KeyValue {
    @Nonnull
    private final DB db;

    public KeyValueSnappy(@Nonnull final DB db) {
        this.db = db;
    }

    @Nonnull
    public static KeyValueSnappy create(@Nonnull Context context,
                                        @Nonnull String name) throws SnappydbException {
        return new KeyValueSnappy(createDb(context, name));
    }

    @Deprecated
    public KeyValueSnappy(@Nonnull Context context,
                          @Nonnull String name) {
        //noinspection deprecation
        this(createDbOrFail(context, name));
    }

    @Deprecated
    private static DB createDbOrFail(@Nonnull Context context,
                                     @Nonnull String name) {
        try {
            return createDb(context, name);
        } catch (SnappydbException e) {
            throw new RuntimeException(e);
        }
    }

    private static DB createDb(@Nonnull Context context, @Nonnull String name) throws SnappydbException {
        return new SnappyDB.Builder(context).name(name).build();
    }

    @Override
    public void put(@Nonnull ByteString key, @Nonnull ByteString value) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(value);
        try {
            db.put(ByteUtils.toString(key), value.toByteArray());
        } catch (SnappydbException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void del(@Nonnull ByteString key) {
        Preconditions.checkNotNull(key);
        try {
            db.del(ByteUtils.toString(key));
        } catch (SnappydbException e) {
            throw new RuntimeException(e);
        }
    }

    @Nonnull
    @Override
    public ByteString getBytes(@Nonnull ByteString key) throws NotFoundException {
        Preconditions.checkNotNull(key);
        try {
            return ByteString.copyFrom(db.getBytes(ByteUtils.toString(key)));
        } catch (SnappydbException e) {
            if (e.getMessage().contains("NotFound")) {
                throw new NotFoundException();
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    @Nonnull
    @Override
    public Iterator getKeys(@Nonnull ByteString prefix, ByteString nextTokenOrNull, int batch) {
        return fetchValues(prefix, nextTokenOrNull, batch);
    }

    @Nonnull
    @Override
    public Iterator fetchValues(@Nonnull final ByteString prefix, @Nullable final ByteString nextTokenOrNull, final int batch) {
        Preconditions.checkNotNull(prefix);
        Preconditions.checkArgument(batch >= 1);
        final int batchQuery = Math.min(batch, 1000);
        final ArrayList<ByteString> arrayList = new ArrayList<>(batchQuery);
        final ByteString startWith = nextTokenOrNull == null ? prefix : nextTokenOrNull;
        final KeyIterator keysIterator = findKeysIterator(startWith);
        try {
            final Iterable<String[]> iterator = keysIterator.byBatch(batchQuery);
            boolean stop = false;
            for (String[] keys : iterator) {
                for (String key1 : keys) {
                    ByteString key = ByteUtils.fromString(key1);
                    if (!key.startsWith(prefix)) {
                        stop = true;
                        break;
                    }
                    if (arrayList.size() == batch) {
                        return new Iterator(arrayList, key);
                    }
                    arrayList.add(getBytes(key));
                }
                if (stop) {
                    break;
                }

            }
        } catch (NotFoundException e) {
            throw new RuntimeException(e);
        } finally {
            keysIterator.close();
        }
        return new Iterator(arrayList, null);
    }

    @Nonnull
    @Override
    public Iterator fetchKeys(@Nonnull final ByteString prefix, @Nullable final ByteString nextTokenOrNull, final int batch) {
        Preconditions.checkNotNull(prefix);
        Preconditions.checkArgument(batch >= 1);
        final int batchQuery = Math.min(batch, 1000);
        final ArrayList<ByteString> arrayList = new ArrayList<>(batchQuery);
        final ByteString startWith = nextTokenOrNull == null ? prefix : nextTokenOrNull;
        final KeyIterator keysIterator = findKeysIterator(startWith);
        try {
            final Iterable<String[]> iterator = keysIterator.byBatch(batchQuery);
            boolean stop = false;
            for (String[] keys : iterator) {
                for (String key1 : keys) {
                    final ByteString key = ByteUtils.fromString(key1);
                    if (!key.startsWith(prefix)) {
                        stop = true;
                        break;
                    }
                    if (arrayList.size() == batch) {
                        return new Iterator(arrayList, key);
                    }
                    arrayList.add(key);
                }
                if (stop) {
                    break;
                }
            }
        } finally {
            keysIterator.close();
        }
        return new Iterator(arrayList, null);
    }

    @Override
    public void close() {
        try {
            db.destroy();
        } catch (SnappydbException e) {
            throw new RuntimeException(e);
        }
    }

    @Nonnull
    private KeyIterator findKeysIterator(@Nonnull ByteString startWith) {
        try {
            return db.findKeysIterator(ByteUtils.toString(startWith));
        } catch (SnappydbException e) {
            throw new RuntimeException(e);
        }
    }
}
