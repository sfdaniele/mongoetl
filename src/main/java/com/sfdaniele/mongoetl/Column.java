package com.sfdaniele.mongoetl;

import com.google.common.base.CaseFormat;
import java.lang.reflect.Field;
import org.mongodb.morphia.annotations.Id;

/** Holds column information so we can copy it to mysql. */
class Column {
  String name;
  String type;
  Object value;

  Column(String name, String type, Object value) {
    this.name = name;
    this.type = type;
    this.value = value;
  }

  static Column from(Field field) {
    String name = columnName(field);
    String type = type(field);
    if (type == null) return null;
    return new Column(name, type, null);
  }

  /** Maps a field to a Mysql type, if supported. */
  private static String type(Field field) {
    if (field.getType() == Integer.class || field.getType() == Long.class) {
      return "BIGINT";
    } else if (EntityReflector.isSimpleReference(field) || field.isAnnotationPresent(Id.class)) {
      return "VARCHAR(100)";
    } else if (field.getType() == Double.class || field.getType() == Float.class) {
      return "DOUBLE";
    } else if (field.getType() == String.class) {
      return "TEXT";
    } else if (field.getType().toString().toLowerCase().equals("boolean")){
      return "BOOLEAN";
    } else {
      return null;
    }
  }

  private static String columnName(Field field) {
    String name = EntityReflector.isSimpleReference(field) ? field.getName() + "_id" : field.getName();
    return CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, name);
  }

  @Override
  public String toString() {
    return name + " " + type + " DEFAULT NULL";
  }
}
