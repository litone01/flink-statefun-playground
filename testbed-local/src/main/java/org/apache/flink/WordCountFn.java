package org.apache.flink;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.apache.flink.statefun.sdk.java.types.SimpleType;
import org.apache.flink.statefun.sdk.java.types.Type;
import org.apache.flink.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.types.Types.MAX;

public class WordCountFn implements StatefulFunction {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountFn.class);
    static final TypeName TYPE = TypeName.typeNameFromString("statefun.testbed.fns/word-count");
    static final ValueSpec<WordFrequency> WORD_FREQUENCY = ValueSpec.named("word_frequency").withCustomType(WordFrequency.TYPE);

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) {
        if (message.is(Types.ADD_NEW_TEXT)) {
            final Types.AddNewText addNewText = message.as(Types.ADD_NEW_TEXT);

            final AddressScopedStorage storage = context.storage();
            final WordFrequency wordFrequency = storage.get(WORD_FREQUENCY).orElse(WordFrequency.initEmpty());

            boolean reachedMax = updateWordFrequency(wordFrequency, addNewText.getText());
            storage.set(WORD_FREQUENCY, wordFrequency);

            if (reachedMax) {
                // Dummy message, do not need to contain any value
                // Note: all messages are sent to the same address, so we can get the global sum
                final Message request = MessageBuilder.forAddress(MaxCountFn.TYPE, MaxCountFn.UNIFIED_KEY).
                        withValue(1).build();
                context.send(request);
            }
        }

        return context.done();
    }

    private boolean updateWordFrequency(WordFrequency wordFrequency, String word) {
        boolean reachedMax = false;

        int updatedFrequency = wordFrequency.updateOrElseCreate(word, 1);
        if (updatedFrequency == MAX) {
            reachedMax = true;
            LOG.info("Word {} frequency reached max {}", word, MAX);
            System.out.println("Word " + word + " frequency reached max " + MAX);
        } else if (updatedFrequency > MAX) {
            LOG.error("Word {} frequency reached max {}", word, MAX);
            System.out.println("Word " + word + " frequency reached max " + MAX);
        }

        return reachedMax;
    }

    private static class WordFrequency {
        private static final ObjectMapper mapper = new ObjectMapper();
        static final Type<WordFrequency> TYPE = SimpleType.simpleImmutableTypeFrom(
                TypeName.typeNameFromString("com.example/WordFrequency"),
                mapper::writeValueAsBytes,
                bytes -> mapper.readValue(bytes, WordFrequency.class));

        @JsonProperty("word_frequency")
        private final Map<String, Integer> wordFrequency;

        public static WordFrequency initEmpty() {
            return new WordFrequency(new HashMap<>());
        }

        @JsonCreator
        public WordFrequency(@JsonProperty("word_frequency") Map<String, Integer> wordFrequency) {
            this.wordFrequency = wordFrequency;
        }

        public int updateOrElseCreate(String word, int frequency) {
            int newFrequency = wordFrequency.getOrDefault(word, 0) + frequency;
            wordFrequency.put(word, newFrequency);

            return newFrequency;
        }

        @JsonIgnore
        public Set<Map.Entry<String, Integer>> getEntries() {
            return wordFrequency.entrySet();
        }

        @JsonProperty("word_frequency")
        public Map<String, Integer> getWordFrequency() {
            return wordFrequency;
        };

        public void clear() {
            wordFrequency.clear();
        }

        @Override
        public String toString() {
            return "WordFrequency{" + "wordFrequency=" + wordFrequency + '}';
        }
    }
}
