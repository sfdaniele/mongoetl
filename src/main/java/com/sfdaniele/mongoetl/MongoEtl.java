package com.sfdaniele.mongoetl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.annotations.Entity;

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;

public class MongoEtl {
  private final static int BATCH_SIZE = 1000;
  private final static String DROP_STATEMENT = "DROP TABLE IF EXISTS %s";
  private final static String CREATE_STATEMENT = "CREATE TABLE %s (%s)";
  private final static String INSERT_STATEMENT = "INSERT INTO %s (%s) VALUES %s";
  private final static String TABLE_NAME_PREFIX = "etl_";

  private final MongoQuery mongoQuery;
  private final SqlSession session;

  /**
   * Copy entities.
   *
   * @param mongoDatastore The data store you want to copy from.
   * @param dataSource The SQL DB (only Mysql has been tested) you want to copy into.
   */
  public MongoEtl(Datastore mongoDatastore, DataSource dataSource) {
    this.mongoQuery = new MongoQuery(mongoDatastore);
    this.session = new SqlSession(dataSource);
  }

  @VisibleForTesting MongoEtl(MongoQuery mongoQuery, SqlSession session) {
    this.mongoQuery = mongoQuery;
    this.session = session;
  }

  /**
   * Call with a list of entities you would like to copy to Mysql. This ETL will attempt to keep
   * foreign references to other entities. However, one to many references are not supported yet.
   */
  public int etl(List<Class<?>> mongoEntities) {
    int totalAdded = 0;
    for (Class<?> clazz : mongoEntities) {
      String tableName = tableName(clazz);

      session.execute(String.format(DROP_STATEMENT, tableName));

      session.execute(String.format(CREATE_STATEMENT, tableName,
          Joiner.on(", ").join(EntityReflector.definitionsFor(clazz))));

      List<?> entities;
      int offset = 0;
      do {
        ImmutableList.Builder<List<Column>> batchInsert = ImmutableList.builder();
        entities = mongoQuery.list(clazz, offset, BATCH_SIZE);
        for (Object mongoEntity : entities) {
          List<Column> columns = EntityReflector.withValues(mongoEntity);
          totalAdded += columns.size();
          batchInsert.add(columns);
        }
        insert(tableName, batchInsert.build());
        offset = offset + BATCH_SIZE;
      } while (!entities.isEmpty());
    }
    return totalAdded;
  }

  private String tableName(Class<?> clazz) {
    return TABLE_NAME_PREFIX +
        LOWER_CAMEL.to(LOWER_UNDERSCORE, clazz.getAnnotation(Entity.class).value());
  }

  private void insert(String tableName, List<List<Column>> batch) {
    if (batch.isEmpty()) return;

    String columnNames = Joiner.on(", ")
        .join(batch.get(0).stream().map(c -> c.name).collect(Collectors.toList()));
    StringBuilder values = new StringBuilder();
    int i = 0;
    for (List<Column> columns : batch) {
      if (i > 0) values.append(",");
      values.append("(");
      values.append((Joiner.on(", ")
          .useForNull("null")
          .join(columns.stream()
              .map(c -> ValueMapper.map(c.value))
              .collect(Collectors.toList()))));
      values.append(")");
      i++;
    }
    String statement = String.format(INSERT_STATEMENT, tableName, columnNames, values);
    session.execute(statement);
  }
}
