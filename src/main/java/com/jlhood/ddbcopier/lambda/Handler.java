package com.jlhood.ddbcopier.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;

import com.jlhood.ddbcopier.DynamoDBCopier;
import com.jlhood.ddbcopier.dagger.AppComponent;
import com.jlhood.ddbcopier.dagger.DaggerAppComponent;

/**
 * Entrypoint for DDB copier lambda.
 */
public class Handler implements RequestHandler<DynamodbEvent, Void> {
    private final DynamoDBCopier copier;

    public Handler() {
        AppComponent component = DaggerAppComponent.create();
        copier = component.dynamoDBCopier();
    }

    @Override
    public Void handleRequest(DynamodbEvent dynamodbEvent, Context context) {
        copier.accept(dynamodbEvent);
        return null;
    }
}
