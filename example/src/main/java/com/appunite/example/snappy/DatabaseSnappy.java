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

import com.appunite.keyvalue.EditOperations;
import com.appunite.keyvalue.KeyGenerator;
import com.appunite.keyvalue.KeyValue;
import com.appunite.keyvalue.NotFoundException;
import com.example.myapplication.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DatabaseSnappy implements Database {

    @Nonnull
    private final KeyValue keyValue;

    public DatabaseSnappy(@Nonnull KeyValue keyValue) {
        this.keyValue = keyValue;
    }

    @Nonnull
    @Override
    public MessageResult getMessageResult(@Nonnull String conversationId, @Nullable MessageResult messageResultOrNull, int batch) {
        if (messageResultOrNull != null) {
            if (messageResultOrNull.getNextToken() == null) {
                throw new IllegalArgumentException("Result does not have more messages");
            }
        }
        final ByteString messageConversationPrefix = getMessageConversationIndex(conversationId);

        final KeyValue.Iterator iterator = keyValue.fetchValues(messageConversationPrefix, messageResultOrNull == null ? null : messageResultOrNull.<ByteString>getNextToken(), batch);

        final List<ByteString> keys = iterator.keys();
        final ArrayList<Message.CommunicationMessage> objects = new ArrayList<>(keys.size());
        for (ByteString key : keys) {
            try {
                objects.add(Message.CommunicationMessage.parseFrom(keyValue.getBytes(key)));
            } catch (InvalidProtocolBufferException | NotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        return new MessageResult(objects, iterator.nextToken());
    }

    @Nonnull
    @Override
    public Message.CommunicationMessage getMessage(@Nonnull ByteString id) throws NotFoundException {
        try {
            return Message.CommunicationMessage.parseFrom(keyValue.getBytes(getMessageKey(id)));
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    private void addMessageOperation(@Nonnull EditOperations operations, @Nonnull Message.CommunicationMessage message) {
        final ByteString messageKey = getMessageKey(message.getId());
        final ByteString key = getMessageConversationIndex(message);
        operations.put(key, messageKey);
        operations.put(messageKey, message.toByteString());
    }

    private void deleteMessageOperation(@Nonnull EditOperations operations, @Nonnull Message.CommunicationMessage message) {
        final Message.CommunicationMessage oldMessage;
        try {
            oldMessage = getMessage(message.getId());
        } catch (NotFoundException e) {
            throw new RuntimeException(e);
        }
        operations.del(getMessageConversationIndex(oldMessage));
        addMessageOperation(operations, message);
    }

    @Override
    public void addMessage(@Nonnull Message.CommunicationMessage message) {
        final KeyValue.Batch batch = keyValue.newBatch();
        addMessageOperation(batch, message);
        batch.write();
    }

    @Override
    public void updateMessage(@Nonnull Message.CommunicationMessage message) {
        final KeyValue.Batch batch = keyValue.newBatch();
        deleteMessageOperation(batch, message);
        batch.write();
    }

    @Override
    public void addMessages(@Nonnull List<Message.CommunicationMessage> messages) {
        final KeyValue.Batch batch = keyValue.newBatch();
        for (Message.CommunicationMessage message : messages) {
            addMessageOperation(batch, message);
        }
        batch.write();
    }

    @Override
    public void updateMessages(@Nonnull List<Message.CommunicationMessage> messages) {
        final KeyValue.Batch batch = keyValue.newBatch();
        for (Message.CommunicationMessage message : messages) {
            deleteMessageOperation(batch, message);
        }
        batch.write();
    }

    private final KeyGenerator keyGenerator = new KeyGenerator();

    private static final byte[] MESSAGE = "message".getBytes();
    private static final byte[] CONVERSATION = "conversation".getBytes();
    private static final byte[] CREATED_AT = "created_at".getBytes();

    @Nonnull
    private ByteString getMessageConversationIndex(@Nonnull String conversationLocalId) {
        return keyGenerator.startIndex(MESSAGE)
                .addField(CONVERSATION, conversationLocalId)
                .buildQuery();
    }

    @Nonnull
    private ByteString getMessageConversationIndex(@Nonnull Message.CommunicationMessage message) {
        return keyGenerator.startIndex(MESSAGE)
                .addField(CONVERSATION, message.getConversationId())
                .addField(CREATED_AT, message.getCreatedAtMillis())
                .buildIndex(message.getId());
    }

    @Nonnull
    private ByteString getMessageKey(@Nonnull ByteString messageId) {
        return keyGenerator.value(MESSAGE, messageId);
    }

    @Override
    public void close() {
        keyValue.close();
    }
}
