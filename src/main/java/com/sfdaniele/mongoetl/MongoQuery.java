package com.sfdaniele.mongoetl;

import java.util.List;
import org.mongodb.morphia.Datastore;

class MongoQuery {
  private final Datastore datastore;

  MongoQuery(Datastore datastore) {
    this.datastore = datastore;
  }

  List list(Class<?> entityClass, int offset, int batchSize) {
    return datastore.find(entityClass)
        .offset(offset)
        .limit(batchSize)
        .asList();
  }
}
