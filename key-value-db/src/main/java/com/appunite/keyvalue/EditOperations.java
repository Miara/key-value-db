package com.appunite.keyvalue;

import com.google.protobuf.ByteString;

import javax.annotation.Nonnull;

public interface EditOperations {
    void put(@Nonnull ByteString key, @Nonnull ByteString value);
    void del(@Nonnull ByteString key);
}
