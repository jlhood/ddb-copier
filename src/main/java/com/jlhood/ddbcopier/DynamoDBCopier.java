package com.jlhood.ddbcopier;

import java.util.Set;
import java.util.function.Consumer;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.Identity;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Translates DynamoDB stream events to writes to the given destination DynamoDB table. Writes the items sequentially according to the order
 * in which they were received from the stream to maintain the order of writes from source table to destination table.
 */
@RequiredArgsConstructor
@Slf4j
public class DynamoDBCopier implements Consumer<DynamodbEvent> {
    public static final String DELETE_EVENT_NAME = "REMOVE";
    public static final Identity TTL_IDENTITY = new Identity().withPrincipalId("dynamodb.amazonaws.com").withType("Service");

    private static final Set<String> ALLOWED_STREAM_VIEW_TYPES = ImmutableSet.of("NEW_IMAGE", "NEW_AND_OLD_IMAGES");

    private final String destinationTable;
    private final AmazonDynamoDB amazonDynamoDB;

    @Override
    public void accept(final DynamodbEvent dynamodbEvent) {
        log.info("Copying {} records", dynamodbEvent.getRecords().size());
        dynamodbEvent.getRecords().forEach(this::processRecord);
    }

    private void processRecord(final DynamodbEvent.DynamodbStreamRecord record) {
        Preconditions.checkState(ALLOWED_STREAM_VIEW_TYPES.contains(record.getDynamodb().getStreamViewType()),
                "ddb-copier requires source table stream to be configured with one of the following StreamViewTypes: " + ALLOWED_STREAM_VIEW_TYPES);

        if (isDelete(record)) {
            if (isTTLDelete(record)) {
                log.info("Ignoring TTL delete on item: {}", record.getDynamodb().getKeys());
                return;
            }
            deleteRecord(record);
        } else {
            putRecord(record);
        }
    }

    private boolean isDelete(final DynamodbEvent.DynamodbStreamRecord record) {
        return DELETE_EVENT_NAME.equals(record.getEventName());
    }

    private boolean isTTLDelete(final DynamodbEvent.DynamodbStreamRecord record) {
        return TTL_IDENTITY.equals(record.getUserIdentity());
    }

    private void deleteRecord(final DynamodbEvent.DynamodbStreamRecord record) {
        log.info("Deleting record: {}", record.getDynamodb().getKeys());
        amazonDynamoDB.deleteItem(destinationTable, record.getDynamodb().getKeys());
    }

    private void putRecord(final DynamodbEvent.DynamodbStreamRecord record) {
        log.info("Creating or updating record: {}", record.getDynamodb().getKeys());
        amazonDynamoDB.putItem(new PutItemRequest()
                .withTableName(destinationTable)
                .withItem(record.getDynamodb().getNewImage()));
    }
}
