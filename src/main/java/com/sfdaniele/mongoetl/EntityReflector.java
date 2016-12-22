package com.sfdaniele.mongoetl;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.NotSaved;
import org.mongodb.morphia.annotations.Reference;

class EntityReflector {
  /** Cache class fields to avoid reflecting on each record. */
  private final static Map<Class, List<Field>> FIELDS = new ConcurrentHashMap<>();

  static List<Column> definitionsFor(Class<?> entityClazz) {
    List<Field> fields = fields(entityClazz);
    ImmutableList.Builder<Column> results = ImmutableList.builder();
    for (Field field : fields) {
      if (field.isAnnotationPresent(NotSaved.class)) continue;
      Column column = Column.from(field);
      // Unsupported type, skip.
      if (column == null) continue;
      results.add(column);
    }
    return results.build();
  }

  static List<Column> withValues(Object mongoEntity) {
    List<Field> fields = fields(mongoEntity.getClass());
    LinkedList<Column> columns = new LinkedList<>();
    for (Field field : fields) {
      if (field.isAnnotationPresent(NotSaved.class)) continue;
      Column column = Column.from(field);
      // If we are missing a column definition, skip.
      if (column == null) continue;
      Object value;

      value = get(mongoEntity, field);
      columns.add(new Column(column.name, column.type, value));
    }
    return columns;
  }

  static boolean isSimpleReference(Field field) {
    return field.isAnnotationPresent(Reference.class)
        && field.getType().isAnnotationPresent(Entity.class);
  }

  static Object entityId(Object entity) {
    for (Field field : fields(entity.getClass())) {
      if (field.isAnnotationPresent(Id.class)) {
        return get(entity, field);
      }
    }
    return null;
  }

  private static Object get(Object entity, Field field) {
    try {
      field.setAccessible(true);
      return field.get(entity);
    } catch (IllegalAccessException e) {
      throw Throwables.propagate(e);
    }
  }

  private static List<Field> fields(Class clazz) {
    return FIELDS.computeIfAbsent(clazz, c -> ImmutableList.copyOf(c.getDeclaredFields()));
  }
}
