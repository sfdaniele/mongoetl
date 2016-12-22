package com.sfdaniele.mongoetl;

import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.NotSaved;
import org.mongodb.morphia.annotations.Reference;

@Entity("test_entity")
public class TestEntity {
  @Id
  private String id;

  @NotSaved
  private String notSaved;

  private String name;

  @Reference
  private AnotherEntity anotherEntity;

  public TestEntity() {
  }

  public TestEntity(String id, String name, AnotherEntity anotherEntity) {
    this.id = id;
    this.name = name;
    this.anotherEntity = anotherEntity;
  }
}
