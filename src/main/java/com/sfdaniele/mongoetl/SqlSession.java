package com.sfdaniele.mongoetl;

import com.google.common.base.Throwables;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;

class SqlSession {
  private final DataSource dataSource;

  SqlSession(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  void execute(String sqlStatement) {
    Connection connection = null;
    try {
      connection = dataSource.getConnection();
      Statement statement = connection.createStatement();
      statement.execute(sqlStatement);
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    } finally {
      try {
        if (connection != null) connection.close();
      } catch (SQLException e) {
        throw Throwables.propagate(e);
      }
    }
  }
}
