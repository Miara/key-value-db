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

import android.content.Context;

import com.appunite.keyvalue.ByteUtils;
import com.appunite.keyvalue.NotFoundException;
import com.example.myapplication.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.realm.Realm;
import io.realm.RealmConfiguration;
import io.realm.RealmObject;
import io.realm.RealmResults;

public class DatabaseRealm implements Database {

    @Nonnull
    private final Realm realm;

    public DatabaseRealm(@Nonnull Context context, @Nonnull String name) {
        realm = Realm.getInstance(new RealmConfiguration.Builder(context).name(name).build());
    }

    @Nonnull
    @Override
    public MessageResult getMessageResult(@Nonnull String conversationId,
                                          @Nullable MessageResult messageResultOrNull,
                                          int batch) {
        if (messageResultOrNull != null) {
            if (messageResultOrNull.getNextToken() == null) {
                throw new RuntimeException("No more data");
            }
        }
        final RealmResults<RealmMessage> result = realm.where(RealmMessage.class)
                .equalTo("conversationId", conversationId)  // implicit AND
                .findAllSorted("createdAt");

        @SuppressWarnings("ConstantConditions")
        int start = messageResultOrNull == null ? 0 : messageResultOrNull.<Integer>getNextToken();

        final List<Message.CommunicationMessage> messages = new ArrayList<>();
        for (int i = start; i < result.size(); i++) {
            if (messages.size() == batch) {
                return new MessageResult(messages, start + batch);
            }
            final RealmMessage message = result.get(i);
            try {
                messages.add(Message.CommunicationMessage.parseFrom(message.getData()));
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }


        return new MessageResult(messages, null);
    }

    @Nonnull
    @Override
    public Message.CommunicationMessage getMessage(@Nonnull ByteString id) throws NotFoundException {
        final RealmMessage message = realm.where(RealmMessage.class)
                .equalTo("id", ByteUtils.toString(id))
                .findFirst();
        if (message == null) {
            throw new NotFoundException();
        }
        try {
            return Message.CommunicationMessage.parseFrom(message.getData());
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addMessage(@Nonnull Message.CommunicationMessage message) {
        realm.beginTransaction();
        final RealmMessage realmMessage = createMessage(message);
        realm.copyToRealm(realmMessage);
        realm.commitTransaction();
    }

    @Nonnull
    private RealmMessage createMessage(@Nonnull Message.CommunicationMessage message) {
        final RealmMessage realmMessage = new RealmMessage();
        realmMessage.setData(message.toByteArray());
        realmMessage.setId(ByteUtils.toString(message.getId()));
        realmMessage.setConversationId(message.getConversationId());
        realmMessage.setCreatedAt(message.getCreatedAtMillis());
        return realmMessage;
    }

    @Override
    public void updateMessage(@Nonnull Message.CommunicationMessage message) {
        realm.beginTransaction();
        final RealmMessage realmMessage = createMessage(message);
        realm.copyToRealmOrUpdate(realmMessage);
        realm.commitTransaction();
    }

    @Override
    public void addMessages(@Nonnull List<Message.CommunicationMessage> messages) {
        realm.beginTransaction();
        final List<RealmObject> operations = new ArrayList<>(messages.size());
        for (Message.CommunicationMessage message : messages) {
            operations.add(createMessage(message));
        }
        realm.copyToRealm(operations);
        realm.commitTransaction();
    }

    @Override
    public void updateMessages(@Nonnull List<Message.CommunicationMessage> messages) {
        realm.beginTransaction();
        final List<RealmObject> operations = new ArrayList<>(messages.size());
        for (Message.CommunicationMessage message : messages) {
            operations.add(createMessage(message));
        }
        realm.copyToRealmOrUpdate(operations);
        realm.commitTransaction();
    }

    @Override
    public void close() {
        realm.close();
    }
}
