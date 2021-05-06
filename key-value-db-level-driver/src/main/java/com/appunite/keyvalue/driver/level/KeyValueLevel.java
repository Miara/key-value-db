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

import android.content.Context;

import com.appunite.keyvalue.KeyValue;
import com.appunite.keyvalue.NotFoundException;
import com.appunite.keyvalue.driver.level.internal.Preconditions;
import com.google.protobuf.ByteString;
import com.snappydb.DB;
import com.snappydb.DBFactory;
import com.snappydb.KeyIterator;
import com.snappydb.SnappyDB;
import com.snappydb.SnappydbException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class KeyValueLevel implements KeyValue {
    @Nonnull
    private final DB db;

    public KeyValueLevel(@Nonnull DB db) {
        this.db = db;
    }

    @Nonnull
    public static KeyValueLevel create(@Nonnull Context context, @Nonnull File path) throws IOException, SnappydbException {
        return new KeyValueLevel(createDb(context, path));
    }

    @Deprecated
    public KeyValueLevel(@Nonnull Context context, @Nonnull File path) {
        this(createDbOrFail(context, path));
    }

    @Deprecated
    @Nonnull
    private static DB createDbOrFail(@Nonnull Context context, @Nonnull File path) {
        try {
            return createDb(context,path);
        } catch (SnappydbException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Nonnull
    private static DB createDb(@Nonnull Context context, @Nonnull File path ) throws IOException, SnappydbException {
        if (!path.isDirectory() && !path.mkdirs()) {
            throw new IOException("Could not create directory");
        }
        return new SnappyDB.Builder(context)
                .directory(path.getAbsolutePath()) //optional
                .build();
    }

    @Override
    public void put(@Nonnull ByteString key, @Nonnull ByteString value) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(value);
        try {
            db.put(key.toString(), value.toByteArray()); //TODO .toByteArray() -> .toString?
        } catch (SnappydbException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void del(@Nonnull ByteString key) {
        Preconditions.checkNotNull(key);
        try {
            db.del(key.toString()); //TODO .toByteArray() -> .toString?
        } catch (SnappydbException e) {
            throw new RuntimeException(e);
        }
    }

    @Nonnull
    @Override
    public Batch newBatch() {
        return new BatchLevel(db);
    }

    private static class BatchLevel implements Batch {

        @Nonnull
        private final DB keyValueLevel;
//        @Nonnull
//        private final WriteBatch writeBatch;

        BatchLevel(@Nonnull DB keyValueLevel) {
            this.keyValueLevel = keyValueLevel;
//            writeBatch = new WriteBatch();
        }

        @Override
        public void put(@Nonnull ByteString key, @Nonnull ByteString value) {
            Preconditions.checkNotNull(key);
            Preconditions.checkNotNull(value);
//            try {
//                writeBatch.putBytes(key.toByteArray(), value.toByteArray());
//            } catch (SnappydbException e) {
//                throw new RuntimeException(e);
//            }
        }

        @Override
        public void del(@Nonnull ByteString key) {
            Preconditions.checkNotNull(key);
//            try {
//                writeBatch.delete(key.toByteArray());
//            } catch (SnappydbException e) {
//                throw new RuntimeException(e);
//            }
        }

        @Override
        public void clear() {
//            writeBatch.clear();
        }

        @Override
        public void write() {
//            try {
//                keyValueLevel.write(writeBatch);
//            } catch (SnappydbException e) {
//                throw new RuntimeException(e);
//            }
        }
    }

    @Nonnull
    @Override
    public ByteString getBytes(@Nonnull ByteString key) throws NotFoundException {
        Preconditions.checkNotNull(key);
        try {
            return ByteString.copyFrom(db.getBytes(key.toString())); //TODO .toByteArray() -> .toString?
        } catch (SnappydbException e) {
            throw new RuntimeException(e);
        }
    }

    @Nonnull
    @Override
    public Iterator getKeys(@Nonnull final ByteString prefix, @Nullable final ByteString nextTokenOrNull, final int batch) {
        return fetchValues(prefix, nextTokenOrNull, batch);
    }

    @Nonnull
    @Override
    public Iterator fetchValues(@Nonnull final ByteString prefix, @Nullable final ByteString nextTokenOrNull, final int batch) {
//        TODO
//        Preconditions.checkNotNull(prefix);
//        Preconditions.checkArgument(batch >= 1);
//        final int batchQuery = Math.min(batch, 1000);
//        final ArrayList<ByteString> arrayList = new ArrayList<>(batchQuery);
//        final ByteString startWith = nextTokenOrNull == null ? prefix : nextTokenOrNull;
//        try {
//            final KeyIterator iterator = db.findKeysIterator(startWith.toString());
//            //noinspection TryFinallyCanBeTryWithResources
//            try {
//                iterator.seekToFirst(startWith.toByteArray());
//                for (iterator.seekToFirst(startWith.toByteArray()); iterator.isValid(); iterator.next()) {
//                    final ByteString key = ByteString.copyFrom(iterator.key());
//                    if (!key.startsWith(prefix)) {
//                        break;
//                    }
//                    if (arrayList.size() == batch) {
//                        return new Iterator(arrayList, key);
//                    }
//                    arrayList.add(ByteString.copyFrom(iterator.value()));
//                }
//            } finally {
//                iterator.close();
//            }
//            return new Iterator(arrayList, null);
//        } catch (SnappydbException e) {
//            throw new RuntimeException(e);
//        }

        final ArrayList<ByteString> arrayList = new ArrayList<>();
        return new Iterator(arrayList, ByteString.EMPTY);
    }

    @Nonnull
    @Override
    public Iterator fetchKeys(@Nonnull final ByteString prefix, @Nullable final ByteString nextTokenOrNull, final int batch) {
//        TODO
//        Preconditions.checkNotNull(prefix);
//        Preconditions.checkArgument(batch >= 1);
//        final int batchQuery = Math.min(batch, 1000);
//        final ArrayList<ByteString> arrayList = new ArrayList<>(batchQuery);
//        final ByteString startWith = nextTokenOrNull == null ? prefix : nextTokenOrNull;
//        try {
//            final LevelIterator iterator = db.newInterator();
//            //noinspection TryFinallyCanBeTryWithResources
//            try {
//                iterator.seekToFirst(startWith.toByteArray());
//                for (iterator.seekToFirst(startWith.toByteArray()); iterator.isValid(); iterator.next()) {
//                    final ByteString key = ByteString.copyFrom(iterator.key());
//                    if (!key.startsWith(prefix)) {
//                        break;
//                    }
//                    if (arrayList.size() == batch) {
//                        return new Iterator(arrayList, key);
//                    }
//                    arrayList.add(key);
//                }
//            } finally {
//                iterator.close();
//            }
//            return new Iterator(arrayList, null);
//        } catch (SnappydbException e) {
//            throw new RuntimeException(e);
//        }

        final ArrayList<ByteString> arrayList = new ArrayList<>();
        return new Iterator(arrayList, ByteString.EMPTY);
    }

    @Override
    public void close() {
        try {
            db.close();
        } catch (SnappydbException e) {
            e.printStackTrace(); //TODO
        }
    }

}