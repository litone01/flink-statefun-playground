package org.example.utils;

import org.example.Identifiers;

public class utils {
  // We encode id by appending streamId in front, i.e. {stream_id}-{original_id}
  public static String getStreamId(String id) {
    return id.split("-")[0];
  }

  // TODO: Currently, hardcoded to a fixed number of "items" topic
  public static String getItemStatusEgressTopic(String streamId) {
    return Identifiers.ITEM_STATUS_TOPICS + "-" + streamId;
  }
}
