package com.sfdaniele.mongoetl;

import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;

@Entity("another_entity")
class AnotherEntity {
  @Id
  private String id;

  AnotherEntity(String id) {
    this.id = id;
  }
}
