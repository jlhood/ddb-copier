package com.jlhood.ddbcopier.dagger;

import javax.inject.Singleton;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;

import com.google.common.base.Preconditions;
import com.jlhood.ddbcopier.DynamoDBCopier;

import dagger.Module;
import dagger.Provides;

/**
 * Application DI wiring.
 */
@Module
public class AppModule {
    @Provides
    @Singleton
    public AmazonDynamoDB provideAmazonDynamoDB() {
        // automatically configured to local region in lambda runtime environment
        return AmazonDynamoDBClientBuilder.standard().build();
    }

    @Provides
    @Singleton
    public DynamoDBCopier provideDynamoDBCopier(final AmazonDynamoDB amazonDynamoDB) {
        String destinationTableName = Env.getDestinationTable();
        Preconditions.checkArgument(destinationTableName != null, String.format("Destination table name not set. Expected environment variable with key %s to contain name of destination table.", Env.DESTINATION_TABLE_KEY));
        return new DynamoDBCopier(destinationTableName, amazonDynamoDB);
    }
}
