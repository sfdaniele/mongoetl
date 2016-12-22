package com.sfdaniele.mongoetl;

import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;

@Entity("another_entity")
public class AnotherEntity {
  @Id
  private String id;

  public AnotherEntity(String id) {
    this.id = id;
  }
}
