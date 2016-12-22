package com.sfdaniele.mongoetl;

import org.apache.commons.lang.StringEscapeUtils;
import org.mongodb.morphia.annotations.Entity;

/** Maps Java values to their corresponding MySql representation. */
class ValueMapper {
  static String map(Object value) {
    if (value == null) return null;
    if (value.getClass().getSimpleName().toLowerCase().equals("boolean")) {
      return (Boolean) value ? "1" : "0";
    } else if (value.getClass() == String.class) {
      return "'" + sanitize(value.toString()) + "'";
    } else if (value.getClass().isAnnotationPresent(Entity.class)) {
      return map(EntityReflector.entityId(value));
    }
    return sanitize(value.toString());
  }

  private static String sanitize(String string) {
    return StringEscapeUtils.escapeSql(string);
  }
}
