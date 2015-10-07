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

package com.appunite.example.snappy;

import android.annotation.TargetApi;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.os.Build;
import android.provider.BaseColumns;
import android.support.annotation.NonNull;

import com.appunite.keyvalue.ByteUtils;
import com.appunite.keyvalue.KeyGenerator;
import com.appunite.keyvalue.NotFoundException;
import com.example.myapplication.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

public class DatabaseSql implements Database {
    private final SQLiteDatabase writableDatabase;

    static class FeedEntry implements BaseColumns {
        public static final String TABLE_NAME = "entry";
        public static final String COLUMN_NAME_ENTRY_ID = "id";
        public static final String COLUMN_NAME_CREATED_AT = "created_at";
        public static final String COLUMN_NAME_CONVERSATION_ID = "conversation_id";
        public static final String COLUMN_NAME_MESSAGE = "message";
        public static final String[] COLUMNS = new String[]{
                COLUMN_NAME_MESSAGE
        };

        public static Message.CommunicationMessage from(Cursor cursor) {
            try {
                return Message.CommunicationMessage.parseFrom(cursor.getBlob(0));
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }


        private static final KeyGenerator keyGenerator = new KeyGenerator();

        public static final byte[] MESSAGE = "message".getBytes();
        public static final byte[] CREATED_AT = "created_at".getBytes();

        @Nonnull
        public static String createdAtKey(Message.CommunicationMessage message) {
            return ByteUtils.toString(keyGenerator.startIndex(MESSAGE).addField(CREATED_AT, message.getCreatedAtMillis())
                    .buildIndex(message.getId()));
        }

        @NonNull
        public static ContentValues createContentValues(Message.CommunicationMessage message) {
            final ContentValues values = new ContentValues();
            values.put(FeedEntry.COLUMN_NAME_ENTRY_ID, ByteUtils.toString(message.getId()));
            values.put(FeedEntry.COLUMN_NAME_CREATED_AT, createdAtKey(message) );
            values.put(FeedEntry.COLUMN_NAME_CONVERSATION_ID, message.getConversationId());
            values.put(FeedEntry.COLUMN_NAME_MESSAGE, message.toByteArray());
            return values;
        }
    }

    public class OpenHelper extends SQLiteOpenHelper {

        private static final String TEXT_TYPE = " TEXT";
        private static final String TEXT_BLOB = " BLOB";
        private static final String TEXT_INTEGER = " INTEGER";
        private static final String COMMA_SEP = ",";
        private static final String SQL_CREATE_ENTRIES =
                "CREATE TABLE " + FeedEntry.TABLE_NAME + " (" +
                        FeedEntry._ID + " INTEGER PRIMARY KEY," +
                        FeedEntry.COLUMN_NAME_ENTRY_ID + TEXT_TYPE + COMMA_SEP +
                        FeedEntry.COLUMN_NAME_CREATED_AT + TEXT_TYPE + COMMA_SEP +
                        FeedEntry.COLUMN_NAME_CONVERSATION_ID + TEXT_INTEGER + COMMA_SEP +
                        FeedEntry.COLUMN_NAME_MESSAGE + TEXT_BLOB +
                        " )";

        private static final String SQL_DELETE_ENTRIES =
                "DROP TABLE IF EXISTS " + FeedEntry.TABLE_NAME;

        public OpenHelper(Context context, String name) {
            super(context, name, null, 1);
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            db.execSQL(SQL_CREATE_ENTRIES);
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            db.execSQL(SQL_DELETE_ENTRIES);
            db.execSQL("CREATE UNIQUE INDEX my_index ON " + FeedEntry.TABLE_NAME + " ( " + FeedEntry.COLUMN_NAME_CONVERSATION_ID + ", " + FeedEntry.COLUMN_NAME_CREATED_AT +  " )");
            onCreate(db);
        }
    }

    public DatabaseSql(Context context, String name) {
        final OpenHelper openHelper = new OpenHelper(context, name);
        writableDatabase = openHelper.getWritableDatabase();
        enableWriteAheadLoggingIfSupported();
    }

    @TargetApi(Build.VERSION_CODES.HONEYCOMB)
    private void enableWriteAheadLoggingIfSupported() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.HONEYCOMB) {
            writableDatabase.enableWriteAheadLogging();
        }
    }

    @Override
    public MessageResult getMessageResult(String conversationId, MessageResult messageResultOrNull, int batch) {
        if (messageResultOrNull != null) {
            if (messageResultOrNull.getNextToken() == null) {
                throw new IllegalArgumentException("Can not query for finished result set");
            }
        }

        final String where;
        final String[] whereArgs;

        if (messageResultOrNull == null) {
            where = FeedEntry.COLUMN_NAME_CONVERSATION_ID + "= ?";
            whereArgs = new String[] {conversationId};
        } else {
            where = FeedEntry.COLUMN_NAME_CONVERSATION_ID + "= ? AND " + FeedEntry.COLUMN_NAME_CREATED_AT + " >= ?";
            whereArgs = new String[] {conversationId, messageResultOrNull.getNextToken()};
        }
        final List<Message.CommunicationMessage> messages = new ArrayList<>();
        final Cursor query = writableDatabase.query(FeedEntry.TABLE_NAME,
                FeedEntry.COLUMNS,
                where,
                whereArgs,
                null,
                null,
                FeedEntry.COLUMN_NAME_CREATED_AT,
                String.valueOf(batch + 1));
        try {
            for (query.moveToFirst(); !query.isAfterLast(); query.moveToNext()) {
                messages.add(FeedEntry.from(query));
            }
            if (messages.size() == batch + 1) {
                final Message.CommunicationMessage nextMessage = messages.remove(batch);
                return new MessageResult(messages, FeedEntry.createdAtKey(nextMessage));
            }
            return new MessageResult(messages, null);
        } finally {
            query.close();
        }
    }

    @Override
    public Message.CommunicationMessage getMessage(ByteString id) throws NotFoundException {
        final Cursor query = writableDatabase.query(FeedEntry.TABLE_NAME,
                FeedEntry.COLUMNS,
                FeedEntry.COLUMN_NAME_ENTRY_ID + " = ?",
                new String[]{ByteUtils.toString(id)}, null, null, null,
                "1");
        try {
            if (!query.moveToFirst()) {
                throw new NotFoundException();
            }
            return FeedEntry.from(query);
        } finally {
            query.close();
        }
    }

    @Override
    public void addMessage(Message.CommunicationMessage message) {
        final ContentValues values = FeedEntry.createContentValues(message);
        writableDatabase.insertOrThrow(FeedEntry.TABLE_NAME, null, values);
    }

    @Override
    public void updateMessage(Message.CommunicationMessage message) {
        final ContentValues values = FeedEntry.createContentValues(message);
        writableDatabase.update(FeedEntry.TABLE_NAME, values, FeedEntry.COLUMN_NAME_ENTRY_ID + " = ?",
                new String[]{ByteUtils.toString(message.getId())});
    }

    @Override
    public void close() {
        writableDatabase.close();
    }
}
