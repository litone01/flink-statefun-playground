/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.types.SimpleType;
import org.apache.flink.statefun.sdk.java.types.Type;

public class Messages {

  private static final ObjectMapper mapper = new ObjectMapper();

  /* ingress -> stock */
  public static final Type<RestockItem> RESTOCK_ITEM_TYPE =
      SimpleType.simpleImmutableTypeFrom(
          TypeName.typeNameFromString("com.example/RestockItem"),
          mapper::writeValueAsBytes,
          bytes -> mapper.readValue(bytes, RestockItem.class));

  /* stock -> egress */
  public static final Type<ItemStatus> STOCK_STATUS_TYPE =
      SimpleType.simpleImmutableTypeFrom(
          TypeName.typeNameFromString("com.example/StockStatus"),
          mapper::writeValueAsBytes,
          bytes -> mapper.readValue(bytes, ItemStatus.class));

  public static class RestockItem {
    private final String itemId;
    private final int quantity;

    @JsonCreator
    public RestockItem(
        @JsonProperty("itemId") String itemId, @JsonProperty("quantity") int quantity) {
      this.itemId = itemId;
      this.quantity = quantity;
    }

    public String getItemId() {
      return itemId;
    }

    public int getQuantity() {
      return quantity;
    }

    @Override
    public String toString() {
      return "RestockItem{" + "itemId='" + itemId + '\'' + ", quantity=" + quantity + '}';
    }
  }

  public static class ItemStatus {

    private final String itemId;
    private final String details;

    public ItemStatus(
        @JsonProperty("itemId") String itemId, @JsonProperty("details") String details) {
      this.itemId = itemId;
      this.details = details;
    }

    public String getItemId() {
      return itemId;
    }

    public String getDetails() {
      return details;
    }

    @Override
    public String toString() {
      return "ItemStatus{" + "itemId='" + itemId + '\'' + ", details='" + details + '\'' + '}';
    }
  }

  // ---------------------------------------------------------------------
  // Internal messages
  // ---------------------------------------------------------------------

  public static final Type<EgressRecord> EGRESS_RECORD_JSON_TYPE =
      SimpleType.simpleImmutableTypeFrom(
          TypeName.typeNameOf("io.statefun.playground", "EgressRecord"),
          mapper::writeValueAsBytes,
          bytes -> mapper.readValue(bytes, EgressRecord.class));

  public static class EgressRecord {
    @JsonProperty("topic")
    private String topic;

    @JsonProperty("payload")
    private String payload;

    public EgressRecord() {
      this(null, null);
    }

    public EgressRecord(String topic, String payload) {
      this.topic = topic;
      this.payload = payload;
    }

    public String getTopic() {
      return topic;
    }

    public String getPayload() {
      return payload;
    }
  }
}
