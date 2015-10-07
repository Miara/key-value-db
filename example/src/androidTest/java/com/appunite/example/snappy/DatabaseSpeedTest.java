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

import android.support.annotation.NonNull;
import android.test.AndroidTestCase;
import android.util.Log;

import com.appunite.keyvalue.IdGenerator;
import com.example.myapplication.Message;
import com.google.common.base.Stopwatch;
import com.snappydb.SnappydbException;

import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;

public class DatabaseSpeedTest extends AndroidTestCase {

    private static final String TAG = DatabaseSpeedTest.class.getCanonicalName();
    private final IdGenerator idGenerator = new IdGenerator();

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }


    public void testSpeed() throws Exception {
        runSpeedTest("snappy", 0, 500, 100);
        runSpeedTest("sqlite", 1, 500, 100);
        runSpeedTest("realm",  2, 500, 100);
        runSpeedTest("memory", 3, 500, 100);

        runSpeedTest("snappy", 0, 500, 200);
        runSpeedTest("sqlite", 1, 500, 200);
        runSpeedTest("realm",  2, 500, 200);
        runSpeedTest("memory", 3, 500, 200);

        runSpeedTest("snappy", 0, 10000, 100);
        runSpeedTest("sqlite", 1, 10000, 100);
        runSpeedTest("realm",  2, 10000, 100);
        runSpeedTest("memory", 3, 10000, 100);
    }

    public void testDeserialize() throws Exception {
        testSerialize(10000);
    }

    private void testSerialize(int sampleSize) throws com.google.protobuf.InvalidProtocolBufferException {
        final ArrayList<Message.CommunicationMessage> messages = prepareMessages("conversation1", sampleSize);

        final ArrayList<byte[]> out1 = new ArrayList<>(messages.size());

        System.gc();
        final Stopwatch stopwatch1 = Stopwatch.createStarted();
        for (Message.CommunicationMessage message : messages) {
            out1.add(message.toByteArray());
        }
        Log.i(TAG, String.format("testSpeed - serialize %d - %s", sampleSize, stopwatch1.toString()));


        final ArrayList<Message.CommunicationMessage> out2 = new ArrayList<>(messages.size());
        System.gc();
        final Stopwatch stopwatch2 = Stopwatch.createStarted();
        for (byte[] message : out1) {
            out2.add(Message.CommunicationMessage.parseFrom(message));
        }
        Log.i(TAG, String.format("testSpeed - deserialize %d - %s", sampleSize, stopwatch2.toString()));

        assertEquals(messages.size(), out2.size());
    }

    private void runSpeedTest(String dbName, int databaseType, int writeSample, int readSample) throws SnappydbException {
        final Database database = DatabaseProvider.provide(getContext(), databaseType, UUID.randomUUID().toString());
        try {
            addSomeNotImportantElements(database, writeSample);

            final ArrayList<Message.CommunicationMessage> list = prepareMessages("conversation10", writeSample);

            System.gc();
            final Stopwatch stopwatch1 = Stopwatch.createStarted();
            for (Message.CommunicationMessage message : list) {
                database.addMessage(message);
            }
            Log.i(TAG, String.format("testSpeed - %s add %d: %s", dbName, writeSample, stopwatch1.toString()));

            final ArrayList<Message.CommunicationMessage> updatedMessages = new ArrayList<>(list.size());
            for (Message.CommunicationMessage message : list) {
                updatedMessages.add(message
                        .toBuilder()
                        .setMessage("2Lorem ipsum dolor sit amet, consectetur adipiscing elit. Aenean vitae varius orci, ac luctus mi.")
                        .build());
            }

            System.gc();
            final Stopwatch stopwatch2 = Stopwatch.createStarted();
            for (Message.CommunicationMessage message : updatedMessages) {
                database.updateMessage(message);
            }
            Log.i(TAG, String.format("testSpeed - %s update %d: %s", dbName, writeSample, stopwatch2.toString()));

            System.gc();
            final Stopwatch stopwatch3 = Stopwatch.createStarted();
            final Database.MessageResult first = database.getMessageResult("conversation10", null, 100);
            Log.i(TAG, String.format("testSpeed - %s read first %d: %s", dbName, readSample, stopwatch3.toString()));
            assertEquals(100, first.getMessages().size());
            assertNotNull(first.getNextToken());

            System.gc();
            final Stopwatch stopwatch4 = Stopwatch.createStarted();
            final Database.MessageResult second = database.getMessageResult("conversation10", first, 100);
            Log.i(TAG, String.format("testSpeed - %s read second %d: %s", dbName, readSample, stopwatch4.toString()));
            assertNotNull(first.getNextToken());
            assertEquals(100, second.getMessages().size());
            assertNotNull(second.getNextToken());
        } finally {
            database.close();
        }
    }


    @NonNull
    private ArrayList<Message.CommunicationMessage> prepareMessages(String conversationId, int writeSample) {
        final Random random = new Random();
        final ArrayList<Message.CommunicationMessage> list = new ArrayList<>();
        for (int i = 0; i < writeSample; ++i) {
            list.add(Message.CommunicationMessage.newBuilder()
                    .setId(idGenerator.newId())
                    .setConversationId(conversationId)
                    .setMessage("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Aenean vitae varius orci, ac luctus mi.")
                    .setCreatedAtMillis(random.nextLong())
                    .build());
        }
        return list;
    }

    private void addSomeNotImportantElements(Database database, int writeSample) throws SnappydbException {
        final ArrayList<Message.CommunicationMessage> list1 = prepareMessages("conversation1", writeSample);
        for (Message.CommunicationMessage message : list1) {
            database.addMessage(message);
        }
        final ArrayList<Message.CommunicationMessage> list2 = prepareMessages("conversation11", writeSample);
        for (Message.CommunicationMessage message : list2) {
            database.addMessage(message);
        }
    }
}
