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
import androidx.annotation.NonNull;
import androidx.test.ext.junit.runners.AndroidJUnit4;
import androidx.test.filters.LargeTest;
import androidx.test.platform.app.InstrumentationRegistry;

import android.util.Log;

import com.appunite.keyvalue.IdGenerator;
import com.example.myapplication.Message;
import com.google.common.base.Stopwatch;
import com.snappydb.SnappydbException;

import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assert_;

@RunWith(AndroidJUnit4.class)
@LargeTest
public class DatabaseSpeedTest {

    private static final String TAG = DatabaseSpeedTest.class.getCanonicalName();
    private final IdGenerator idGenerator = new IdGenerator();

    @Test
    public void testSpeed() throws Exception {
        allTests(500, 100, false);
        allTests(500, 200, false);
        allTests(10000, 100, false);
        allTests(500, 100, true);
        allTests(500, 200, true);
        allTests(10000, 100, true);
    }

    @Test
    public void testSpeedOnlySnappyAndLevelDb() throws Exception {
        levelDbTests(500, 100, false);
        levelDbTests(500, 200, false);
        levelDbTests(10000, 100, false);
        levelDbTests(500, 100, true);
        levelDbTests(500, 200, true);
        levelDbTests(10000, 100, true);
    }

    private void levelDbTests(int writeSample, int readSample, boolean useBatch) throws Exception {
        runSpeedTest("snappy", 0, writeSample, readSample, useBatch);
        runSpeedTest("leveldb", 4, writeSample, readSample, useBatch);
    }

    private void allTests(int writeSample, int readSample, boolean useBatch) throws Exception {
        runSpeedTest("snappy", 0, writeSample, readSample, useBatch);
        runSpeedTest("sqlite", 1, writeSample, readSample, useBatch);
        runSpeedTest("leveldb", 4, writeSample, readSample, useBatch);
        runSpeedTest("realm", 2, writeSample, readSample, useBatch);
        runSpeedTest("memory", 3, writeSample, readSample, useBatch);
    }

    @Test
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

        assertThat(messages.size()).isEqualTo(out2.size());
    }

    private void runSpeedTest(String dbName, int databaseType, int writeSample, int readSample, boolean useBatch) throws Exception {
        final Context targetContext = InstrumentationRegistry.getInstrumentation().getTargetContext();
        final Database database = DatabaseProvider.provide(targetContext, databaseType, UUID.randomUUID().toString());
        try {
            addSomeNotImportantElements(database, writeSample);

            final ArrayList<Message.CommunicationMessage> list = prepareMessages("conversation10", writeSample);

            System.gc();
            final Stopwatch stopwatch1 = Stopwatch.createStarted();
            if (useBatch) {
                database.addMessages(list);
            } else {
                for (Message.CommunicationMessage message : list) {
                    database.addMessage(message);
                }
            }
            Log.i(TAG, String.format("testSpeed - %s add %d: %s, inBatch: %s", dbName, writeSample, stopwatch1.toString(), useBatch));

            final ArrayList<Message.CommunicationMessage> updatedMessages = new ArrayList<>(list.size());
            for (Message.CommunicationMessage message : list) {
                updatedMessages.add(message
                        .toBuilder()
                        .setMessage("2Lorem ipsum dolor sit amet, consectetur adipiscing elit. Aenean vitae varius orci, ac luctus mi.")
                        .build());
            }

            System.gc();
            final Stopwatch stopwatch2 = Stopwatch.createStarted();
            if (useBatch) {
                database.updateMessages(updatedMessages);
            } else {
                for (Message.CommunicationMessage message : updatedMessages) {
                    database.updateMessage(message);
                }
            }
            Log.i(TAG, String.format("testSpeed - %s update %d: %s, inBatch: %s", dbName, writeSample, stopwatch2.toString(), useBatch));

            System.gc();
            final Stopwatch stopwatch3 = Stopwatch.createStarted();
            final Database.MessageResult first = database.getMessageResult("conversation10", null, 100);
            Log.i(TAG, String.format("testSpeed - %s read first %d: %s, inBatch: %s", dbName, readSample, stopwatch3.toString(), useBatch));
            assertThat(first.getMessages()).hasSize(100);
            assertThat(first.getNextToken()).isNotNull();

            System.gc();
            final Stopwatch stopwatch4 = Stopwatch.createStarted();
            final Database.MessageResult second = database.getMessageResult("conversation10", first, 100);
            Log.i(TAG, String.format("testSpeed - %s read second %d: %s, inBatch: %s", dbName, readSample, stopwatch4.toString(), useBatch));
            assertThat(second.getMessages()).hasSize(100);
            assertThat(second.getNextToken()).isNotNull();
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
