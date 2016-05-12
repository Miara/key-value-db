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

import com.appunite.keyvalue.driver.level.internal.Preconditions;
import com.appunite.keyvalue.KeyValue;
import com.appunite.keyvalue.NotFoundException;
import com.appunite.leveldb.KeyNotFoundException;
import com.appunite.leveldb.LevelDB;
import com.appunite.leveldb.LevelDBException;
import com.appunite.leveldb.LevelIterator;
import com.google.protobuf.ByteString;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import javax.annotation.Nonnull;

public class KeyValueLevel implements KeyValue {
    @Nonnull
    private final LevelDB db;

    public KeyValueLevel(@Nonnull File path) {
        try {
            if (!path.isDirectory() && !path.mkdirs()) {
                throw new IOException("Could not create directory");
            }
            db = new LevelDB(path.getAbsolutePath());
        } catch (LevelDBException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(@Nonnull ByteString key, @Nonnull ByteString value) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(value);
        try {
            db.putBytes(key.toByteArray(), value.toByteArray());
        } catch (LevelDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void del(@Nonnull ByteString key) {
        Preconditions.checkNotNull(key);
        try {
            db.delete(key.toByteArray());
        } catch (LevelDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Nonnull
    @Override
    public ByteString getBytes(@Nonnull ByteString key) throws NotFoundException {
        Preconditions.checkNotNull(key);
        try {
            return ByteString.copyFrom(db.getBytes(key.toByteArray()));
        } catch (LevelDBException e) {
            throw new RuntimeException(e);
        } catch (KeyNotFoundException e) {
            throw new NotFoundException();
        }
    }

    @Nonnull
    @Override
    public Iterator getKeys(@Nonnull ByteString prefix, ByteString nextTokenOrNull, int batch) {
        Preconditions.checkNotNull(prefix);
        Preconditions.checkArgument(batch >= 1);
        final int batchQuery = Math.min(batch, 1000);
        final ArrayList<ByteString> arrayList = new ArrayList<>(batchQuery);
        final ByteString startWith = nextTokenOrNull == null ? prefix : nextTokenOrNull;
        try {
            final LevelIterator iterator = db.newInterator();
            //noinspection TryFinallyCanBeTryWithResources
            try {
                iterator.seekToFirst(startWith.toByteArray());
                for (iterator.seekToFirst(startWith.toByteArray()); iterator.isValid(); iterator.next()) {
                    final ByteString key = ByteString.copyFrom(iterator.key());
                    if (!key.startsWith(prefix)) {
                        break;
                    }
                    if (arrayList.size() == batch) {
                        return new Iterator(arrayList, key);
                    }
                    arrayList.add(ByteString.copyFrom(iterator.value()));
                }
            } finally {
                iterator.close();
            }
            return new Iterator(arrayList, null);
        } catch (LevelDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        db.close();
    }

}
