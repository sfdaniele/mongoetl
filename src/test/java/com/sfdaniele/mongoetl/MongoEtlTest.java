package com.sfdaniele.mongoetl;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MongoEtlTest {
  private SqlSession session;

  private final String EXPECTED_SCHEMA = "CREATE TABLE etl_test_entity "
      + "(id VARCHAR(100) DEFAULT NULL, name TEXT DEFAULT NULL, "
      + "another_entity_id VARCHAR(100) DEFAULT NULL)";

  @Test
  public void emptyEntity() throws Exception {
    MongoEtl mongoEtl = returningEntity(new TestEntity());
    int totalAdded = mongoEtl.etl(ImmutableList.of(TestEntity.class));
    assertThat(totalAdded).isEqualTo(3);
    verify(session).execute("DROP TABLE IF EXISTS etl_test_entity");
    verify(session).execute(EXPECTED_SCHEMA);
    verify(session).execute("INSERT INTO etl_test_entity (id, name, another_entity_id) "
        + "VALUES (null, null, null)");
  }

  @Test
  public void nonEmptyEntity() throws Exception {
    MongoEtl mongoEtl = returningEntity(new TestEntity("123", "Name", new AnotherEntity("abc")));
    int totalAdded = mongoEtl.etl(ImmutableList.of(TestEntity.class));
    assertThat(totalAdded).isEqualTo(3);
    verify(session).execute("DROP TABLE IF EXISTS etl_test_entity");
    verify(session).execute(EXPECTED_SCHEMA);
    verify(session).execute("INSERT INTO etl_test_entity (id, name, another_entity_id) "
        + "VALUES ('123', 'Name', 'abc')");
  }

  @Test
  public void sanitize() throws Exception {
    MongoEtl mongoEtl = returningEntity(new TestEntity("123", "'Name'", new AnotherEntity("abc")));
    int totalAdded = mongoEtl.etl(ImmutableList.of(TestEntity.class));
    assertThat(totalAdded).isEqualTo(3);
    verify(session).execute("DROP TABLE IF EXISTS etl_test_entity");
    verify(session).execute(EXPECTED_SCHEMA);
    verify(session).execute("INSERT INTO etl_test_entity (id, name, another_entity_id) "
        + "VALUES ('123', '''Name''', 'abc')");
  }

  private MongoEtl returningEntity(TestEntity testEntity) {
    session = Mockito.mock(SqlSession.class);
    MongoQuery mongoQuery = Mockito.mock(MongoQuery.class);
    when(mongoQuery.list(any(), Matchers.eq(0), anyInt())).thenReturn(ImmutableList.of(testEntity));
    return new MongoEtl(mongoQuery, session);
  }
}