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

import static org.example.Messages.*;

import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.java.AddressScopedStorage;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.ValueSpec;
import org.apache.flink.statefun.sdk.java.message.EgressMessage;
import org.apache.flink.statefun.sdk.java.message.EgressMessageBuilder;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.example.utils.utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class StockFn implements StatefulFunction {

  private static final Logger LOG = LoggerFactory.getLogger(StockFn.class);

  static final TypeName TYPE = TypeName.typeNameFromString("com.example/stock");
  static final ValueSpec<Integer> STOCK = ValueSpec.named("stock").withIntType();

  @Override
  public CompletableFuture<Void> apply(Context context, Message message) {
    AddressScopedStorage storage = context.storage();
    final int quantity = storage.get(STOCK).orElse(0);
    if (message.is(RESTOCK_ITEM_TYPE)) {
      RestockItem restock = message.as(RESTOCK_ITEM_TYPE);

      LOG.info("Received: {}", restock);
      LOG.info("Scope: {}", context.self());

      // Update the item quantity
      final int newQuantity = quantity + restock.getQuantity();
      storage.set(STOCK, newQuantity);
      String itemId = restock.getItemId();
      LOG.info("ItemId: {}, New Quantity: {}", itemId, newQuantity);

      // Send the new status to the egress
      String streamId = utils.getStreamId(itemId);
      String egressTopic = utils.getItemStatusEgressTopic(streamId);
      final ItemStatus itemStatus = createItemStatus(itemId, newQuantity);
      final EgressMessage egressMessage =
          EgressMessageBuilder.forEgress(Identifiers.ITEM_STATUS_EGRESS)
              .withCustomType(
                  Messages.EGRESS_RECORD_JSON_TYPE,
                  new EgressRecord(egressTopic, itemStatus.toString()))
              .build();
      context.send(egressMessage);

    } else {
      LOG.error("Unknown message type: {}; message: {}", message.valueTypeName(), message);
    }

    LOG.info("---");
    return context.done();
  }

  private ItemStatus createItemStatus(String itemId, int quantity) {
    String details = String.format("quantity: %d", quantity);

    return new ItemStatus(itemId, details);
  }
}
