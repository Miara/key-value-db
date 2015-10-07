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

import com.appunite.keyvalue.NotFoundException;
import com.example.myapplication.Message.CommunicationMessage;
import com.google.protobuf.ByteString;

import java.util.List;

public interface Database {

    class MessageResult {
        private final List<CommunicationMessage> messages;
        private final Object nextToken;

        public MessageResult(List<CommunicationMessage> messages, Object nextToken) {
            this.messages = messages;
            this.nextToken = nextToken;
        }

        public boolean isLast() {
            return nextToken == null;
        }

        public List<CommunicationMessage> getMessages() {
            return messages;
        }

        protected  <T> T getNextToken() {
            //noinspection unchecked
            return (T)nextToken;
        }
    }

    MessageResult getMessageResult(String conversationId, MessageResult messageResultOrNull, int batch);

    CommunicationMessage getMessage(ByteString id) throws NotFoundException;

    void addMessage(CommunicationMessage message);

    void updateMessage(CommunicationMessage message);

    void close();
}
