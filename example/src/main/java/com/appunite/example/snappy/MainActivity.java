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

import android.app.Activity;
import android.os.Bundle;
import android.widget.TextView;

import com.appunite.keyvalue.KeyValue;
import com.appunite.keyvalue.NotFoundException;
import com.appunite.keyvalue.driver.level.KeyValueLevel;
import com.example.myapplication.Message;
import com.google.protobuf.ByteString;
import com.snappydb.SnappydbException;

import java.io.IOException;

public class MainActivity extends Activity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.main_activity);

        final KeyValue keyValue;
        try {
            keyValue = KeyValueLevel.create(this, getDatabasePath("my.leveldb"));
        } catch (IOException | SnappydbException e) {
            throw new RuntimeException(e);
        }
        final DatabaseSnappy databaseSnappy = new DatabaseSnappy(keyValue);
        databaseSnappy.addMessage(Message.CommunicationMessage.newBuilder()
                .setId(ByteString.copyFromUtf8("id1"))
                .setCreatedAtMillis(123L)
                .setConversationId("id1")
                .setMessage("ala ma kota")
                .build());
        try {
            final Message.CommunicationMessage message = databaseSnappy.getMessage(ByteString.copyFromUtf8("id1"));
            final TextView textView = (TextView) findViewById(R.id.main_activity_text);
            textView.setText(message.getMessage());
        } catch (NotFoundException e) {
            throw new RuntimeException(e);
        }
        databaseSnappy.close();
    }
}
