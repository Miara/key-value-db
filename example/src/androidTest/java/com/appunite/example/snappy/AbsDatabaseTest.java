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

import android.test.AndroidTestCase;

import com.appunite.keyvalue.IdGenerator;
import com.appunite.keyvalue.NotFoundException;
import com.example.myapplication.Message;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;

import java.util.List;
import java.util.UUID;

public abstract class AbsDatabaseTest extends AndroidTestCase {

    private final IdGenerator idGenerator = new IdGenerator();
    private Database database;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        database = DatabaseProvider.provide(getContext(), getDatabase(), UUID.randomUUID().toString());
    }

    protected abstract int getDatabase();

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

        assertEquals(id, retMessage.getId());
        assertEquals("conversationId", retMessage.getConversationId());
        assertEquals("message", retMessage.getMessage());
        assertEquals(123, retMessage.getCreatedAtMillis());
    }

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

        assertEquals(1, messages.size());
        assertEquals("conversation2", messages.get(0).getConversationId());
        assertEquals("correct", messages.get(0).getMessage());
    }

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

        assertEquals(1, messages1.getMessages().size());
        assertEquals(1, messages2.getMessages().size());

        final String firstTitle = messages1.getMessages().get(0).getMessage();
        final String secondTitle = messages2.getMessages().get(0).getMessage();
        final ImmutableSet<String> test = ImmutableSet.of(firstTitle, secondTitle);
        assertEquals(ImmutableSet.of("message1", "message2"), test);
    }

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
        assertEquals(3, messages.size());
        assertEquals(123, messages.get(0).getCreatedAtMillis());
        assertEquals(124, messages.get(1).getCreatedAtMillis());
        assertEquals(125, messages.get(2).getCreatedAtMillis());
    }

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

        assertEquals(2, messages.getMessages().size());
        assertEquals(123, messages.getMessages().get(0).getCreatedAtMillis());
        assertEquals(124, messages.getMessages().get(1).getCreatedAtMillis());
        assertFalse(messages.isLast());


        final Database.MessageResult messages2 = database
                .getMessageResult("conversation2", messages, 2);
        assertEquals(1, messages2.getMessages().size());
        assertEquals(125, messages2.getMessages().get(0).getCreatedAtMillis());
        assertTrue(messages2.isLast());
    }

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

        assertEquals(2, messages.size());
    }

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

        assertEquals(1, messages.size());
        assertEquals("new", messages.get(0).getMessage());
        assertEquals(124, messages.get(0).getCreatedAtMillis());
    }

    public void testAfterGettingNotExistingMessage_throwsException() throws Exception {
        try {
            database.getMessage(ByteString.copyFrom("x", Charsets.UTF_8.name()));
            fail("Not thrown");
        } catch (NotFoundException ignore) {
        }
    }

    @Override
    public void tearDown() throws Exception {
        database.close();
        super.tearDown();
    }
}
