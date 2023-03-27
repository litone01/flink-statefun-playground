package org.apache.flink;

import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.ValueSpec;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class MaxCountFn implements StatefulFunction {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountFn.class);
    static final TypeName TYPE = TypeName.typeNameFromString("statefun.testbed.fns/max-count");
    static final ValueSpec<Integer> MAX_COUNT = ValueSpec.named("max_count").withIntType();
    static final String UNIFIED_KEY = "0";
    @Override
    public CompletableFuture<Void> apply(Context context, Message message) {
        final int maxCount = context.storage().get(MAX_COUNT).orElse(0);
        context.storage().set(MAX_COUNT, maxCount + 1);
        LOG.info("Max count: {}", maxCount);
        System.out.println("Max count: " + maxCount);
        return context.done();
    }
}
