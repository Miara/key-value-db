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
import android.support.test.InstrumentationRegistry;

import com.appunite.keyvalue.IdGenerator;
import com.appunite.keyvalue.NotFoundException;
import com.example.myapplication.Message;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.google.common.truth.Truth.assert_;

public abstract class AbsDatabaseTest {

    private final IdGenerator idGenerator = new IdGenerator();
    private Database database;

    @Before
    public void setUp() throws Exception {
        final Context targetContext = InstrumentationRegistry.getTargetContext();
        database = DatabaseProvider.provide(targetContext, getDatabase(), UUID.randomUUID().toString());
    }

    protected abstract int getDatabase();

    @Test
    public void testAfterWritingElementToDb_itCanBeRetrieved() throws Exception {
        final ByteString id = ByteString.copyFrom("x", Charsets.UTF_8.name());
        final Message.CommunicationMessage message = Message.CommunicationMessage.newBuilder()
                .setId(id)
                .setConversationId("conversationId")
                .setMessage("message")
                .setCreatedAtMillis(123)
                .build();

        database.addMessage(message);

        final Message.CommunicationMessage retMessage = database.getMessage(id);

        assert_().that(retMessage.getId()).isEqualTo(id);
        assert_().that(retMessage.getConversationId()).isEqualTo("conversationId");
        assert_().that(retMessage.getMessage()).isEqualTo("message");
        assert_().that(retMessage.getCreatedAtMillis()).isEqualTo(123);
    }

    @Test
    public void testWhenThereAreSeveralConversations_getMessagesOnlyFromOne() throws Exception {
        database.addMessage(Message.CommunicationMessage.newBuilder()
                .setId(idGenerator.newId())
                .setConversationId("conversation1")
                .setMessage("message")
                .setCreatedAtMillis(123)
                .build());
        database.addMessage(Message.CommunicationMessage.newBuilder()
                .setId(idGenerator.newId())
                .setConversationId("conversation2")
                .setMessage("correct")
                .setCreatedAtMillis(123)
                .build());
        database.addMessage(Message.CommunicationMessage.newBuilder()
                .setId(idGenerator.newId())
                .setConversationId("conversation3")
                .setMessage("message")
                .setCreatedAtMillis(123)
                .build());

        final List<Message.CommunicationMessage> messages = database.getMessageResult("conversation2", null, 100)
                .getMessages();

        assert_().that(messages.size()).isEqualTo(1);
        assert_().that(messages.get(0).getConversationId()).isEqualTo("conversation2");
        assert_().that(messages.get(0).getMessage()).isEqualTo("correct");
    }

    @Test
    public void testWhenThereAreTwoMessagesWithSameId_returnBooth() throws Exception {
        database.addMessage(Message.CommunicationMessage.newBuilder()
                .setId(idGenerator.newId())
                .setConversationId("conversation2")
                .setMessage("message1")
                .setCreatedAtMillis(123)
                .build());
        database.addMessage(Message.CommunicationMessage.newBuilder()
                .setId(idGenerator.newId())
                .setConversationId("conversation2")
                .setMessage("message2")
                .setCreatedAtMillis(123)
                .build());

        final Database.MessageResult messages1 = database.getMessageResult("conversation2", null, 1);
        final Database.MessageResult messages2 = database.getMessageResult("conversation2", messages1, 1);

        assert_().that(messages1.getMessages().size()).isEqualTo(1);
        assert_().that(messages2.getMessages().size()).isEqualTo(1);

        final String firstTitle = messages1.getMessages().get(0).getMessage();
        final String secondTitle = messages2.getMessages().get(0).getMessage();
        final ImmutableSet<String> test = ImmutableSet.of(firstTitle, secondTitle);
        assert_().that((Iterable<String>)test).isEqualTo(ImmutableSet.of("message1", "message2"));
    }

    @Test
    public void testWhenThereAreTwoMessagesWithSameIdBatch_returnBooth() throws Exception {
        final List<Message.CommunicationMessage> messages = new ArrayList<>();
        messages.add(Message.CommunicationMessage.newBuilder()
                .setId(idGenerator.newId())
                .setConversationId("conversation2")
                .setMessage("message1")
                .setCreatedAtMillis(123)
                .build());
        messages.add(Message.CommunicationMessage.newBuilder()
                .setId(idGenerator.newId())
                .setConversationId("conversation2")
                .setMessage("message2")
                .setCreatedAtMillis(123)
                .build());
        database.addMessages(messages);

        final Database.MessageResult messages1 = database.getMessageResult("conversation2", null, 1);
        final Database.MessageResult messages2 = database.getMessageResult("conversation2", messages1, 1);

        assert_().that(messages1.getMessages().size()).isEqualTo(1);
        assert_().that(messages2.getMessages().size()).isEqualTo(1);

        final String firstTitle = messages1.getMessages().get(0).getMessage();
        final String secondTitle = messages2.getMessages().get(0).getMessage();
        final ImmutableSet<String> test = ImmutableSet.of(firstTitle, secondTitle);
        assert_().that((Iterable<String>)test).isEqualTo(ImmutableSet.of("message1", "message2"));
    }

    @Test
    public void testWhenRetrieveMessages_returnAllInOrder() throws Exception {
        database.addMessage(Message.CommunicationMessage.newBuilder()
                .setId(idGenerator.newId())
                .setConversationId("conversation2")
                .setMessage("correct")
                .setCreatedAtMillis(123)
                .build());
        database.addMessage(Message.CommunicationMessage.newBuilder()
                .setId(idGenerator.newId())
                .setConversationId("conversation2")
                .setMessage("correct")
                .setCreatedAtMillis(124)
                .build());
        database.addMessage(Message.CommunicationMessage.newBuilder()
                .setId(idGenerator.newId())
                .setConversationId("conversation2")
                .setMessage("correct")
                .setCreatedAtMillis(125)
                .build());

        final List<Message.CommunicationMessage> messages = database
                .getMessageResult("conversation2", null, 100)
                .getMessages();
        assert_().that(messages.size()).isEqualTo(3);
        assert_().that(messages.get(0).getCreatedAtMillis()).isEqualTo(123);
        assert_().that(messages.get(1).getCreatedAtMillis()).isEqualTo(124);
        assert_().that(messages.get(2).getCreatedAtMillis()).isEqualTo(125);
    }

    @Test
    public void testWhenRetrieveMessagesPartially_returnAllInOrder() throws Exception {
        database.addMessage(Message.CommunicationMessage.newBuilder()
                .setId(idGenerator.newId())
                .setConversationId("conversation2")
                .setMessage("correct")
                .setCreatedAtMillis(123)
                .build());
        database.addMessage(Message.CommunicationMessage.newBuilder()
                .setId(idGenerator.newId())
                .setConversationId("conversation2")
                .setMessage("correct")
                .setCreatedAtMillis(124)
                .build());
        database.addMessage(Message.CommunicationMessage.newBuilder()
                .setId(idGenerator.newId())
                .setConversationId("conversation2")
                .setMessage("correct")
                .setCreatedAtMillis(125)
                .build());
        final Database.MessageResult messages = database
                .getMessageResult("conversation2", null, 2);

        assert_().that(messages.getMessages().size()).isEqualTo(2);
        assert_().that(messages.getMessages().get(0).getCreatedAtMillis()).isEqualTo(123);
        assert_().that(messages.getMessages().get(1).getCreatedAtMillis()).isEqualTo(124);
        assert_().that(messages.isLast()).isFalse();


        final Database.MessageResult messages2 = database
                .getMessageResult("conversation2", messages, 2);
        assert_().that(messages2.getMessages().size()).isEqualTo(1);
        assert_().that(messages2.getMessages().get(0).getCreatedAtMillis()).isEqualTo(125);
        assert_().that(messages2.isLast()).isTrue();
    }

    @Test
    public void testWhenThereAreTwoMessagesWithSameTimestamp_returnBooth() throws Exception {
        database.addMessage(Message.CommunicationMessage.newBuilder()
                .setId(idGenerator.newId())
                .setConversationId("conversation2")
                .setMessage("correct")
                .setCreatedAtMillis(123)
                .build());
        database.addMessage(Message.CommunicationMessage.newBuilder()
                .setId(idGenerator.newId())
                .setConversationId("conversation2")
                .setMessage("correct")
                .setCreatedAtMillis(124)
                .build());

        final List<Message.CommunicationMessage> messages = database.getMessageResult("conversation2", null, 100).getMessages();

        assert_().that(messages).hasSize(2);
    }

    @Test
    public void testAfterUpdateMessage_returnOne() throws Exception {
        final ByteString id = idGenerator.newId();
        final Message.CommunicationMessage message = Message.CommunicationMessage.newBuilder()
                .setId(id)
                .setConversationId("conversation2")
                .setMessage("old")
                .setCreatedAtMillis(123)
                .build();
        database.addMessage(message);

        database.updateMessage(message.toBuilder()
                .setMessage("new")
                .setCreatedAtMillis(124)
                .build());

        final List<Message.CommunicationMessage> messages = database
                .getMessageResult("conversation2", null, 100)
                .getMessages();

        assert_().that(messages).hasSize(1);
        assert_().that(messages.get(0).getMessage()).isEqualTo("new");
        assert_().that(messages.get(0).getCreatedAtMillis()).isEqualTo(124);
    }

    @Test
    public void testAfterUpdateTwoMessagesInBatch_returnBoth() throws Exception {
        final ByteString id1 = idGenerator.newId();
        final Message.CommunicationMessage message1 = Message.CommunicationMessage.newBuilder()
                .setId(id1)
                .setConversationId("conversation2")
                .setMessage("old")
                .setCreatedAtMillis(123)
                .build();
        database.addMessage(message1);

        final ByteString id2 = idGenerator.newId();
        final Message.CommunicationMessage message2 = Message.CommunicationMessage.newBuilder()
                .setId(id2)
                .setConversationId("conversation2")
                .setMessage("old")
                .setCreatedAtMillis(125)
                .build();
        database.addMessage(message2);

        final List<Message.CommunicationMessage> updateMessages = new ArrayList<>();
        updateMessages.add(message1.toBuilder()
                .setMessage("new1")
                .setCreatedAtMillis(124)
                .build());
        updateMessages.add(message2.toBuilder()
                .setMessage("new2")
                .setCreatedAtMillis(126)
                .build());
        database.updateMessages(updateMessages);

        final List<Message.CommunicationMessage> messages = database
                .getMessageResult("conversation2", null, 100)
                .getMessages();

        assert_().that(messages).hasSize(2);
        assert_().that(messages.get(0).getMessage()).isEqualTo("new1");
        assert_().that(messages.get(0).getCreatedAtMillis()).isEqualTo(124);
        assert_().that(messages.get(1).getMessage()).isEqualTo("new2");
        assert_().that(messages.get(1).getCreatedAtMillis()).isEqualTo(126);
    }

    @Test(expected = NotFoundException.class)
    public void testAfterGettingNotExistingMessage_throwsException() throws Exception {
        database.getMessage(ByteString.copyFrom("x", Charsets.UTF_8.name()));
    }

    @After
    public void tearDown() throws Exception {
        database.close();
    }

}
