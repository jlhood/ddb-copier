package com.jlhood.ddbcopier.dagger;

import javax.inject.Singleton;

import com.jlhood.ddbcopier.DynamoDBCopier;

import dagger.Component;

/**
 * Application DI component.
 */
@Singleton
@Component(modules = AppModule.class)
public interface AppComponent {
    DynamoDBCopier dynamoDBCopier();
}
