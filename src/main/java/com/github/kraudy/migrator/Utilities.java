package com.github.kraudy.migrator;

import com.ibm.as400.access.User;

import com.ibm.as400.access.User;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.File;
import java.sql.Connection;

public class Utilities {
  private final Connection connection;
  private final User currentUser;
  private final boolean interactive;

  public Utilities(Connection connection, User currentUser, boolean interactive) {
    this.connection = connection;
    this.currentUser = currentUser;
    this.interactive = interactive;
  }

  public void createDirectory(String dirPath) {
    File outputDir = new File(dirPath);
    if (!outputDir.exists()) {
      System.out.println("Creating dir: " + dirPath + " ...");
      outputDir.mkdirs();
    }
  }

  public String getSystemName() throws SQLException {
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT CURRENT_SERVER AS Server FROM SYSIBM. SYSDUMMY1")) {
      if (rs.next()) {
        return rs.getString("Server").trim();
      }
    }
    return "UNKNOWN";
  }

  public String getCcsid() throws SQLException {
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(
            "Select CCSID From QSYS2. SYSCOLUMNS WHERE TABLE_NAME = 'SYSPARTITIONSTAT'" +
                "And TABLE_SCHEMA = 'QSYS2' And COLUMN_NAME = 'SYSTEM_TABLE_NAME' ")) {
      if (rs.next()) {
        return rs.getString("CCSID").trim();
      }
    }
    return "";
  }

  public String getSourcePFs(String sourcePf, String library) throws SQLException {
    if (!sourcePf.isEmpty()) {
      // Validate if Source PF exists
      try (Statement validateStmt = connection.createStatement();
          ResultSet validateRs = validateStmt.executeQuery(
              "SELECT 1 AS Exist FROM QSYS2. SYSPARTITIONSTAT " +
                  "WHERE SYSTEM_TABLE_SCHEMA = '" + library + "' " +
                  "AND SYSTEM_TABLE_NAME = '" + sourcePf + "' " +
                  "AND TRIM(SOURCE_TYPE) <> '' LIMIT 1")) {
        if (!validateRs.next()) {
          if (interactive) {
            System.out.println(" *Source PF " + sourcePf + " does not exist in library " + library);
            return "";
          }
          throw new IllegalArgumentException("Source PF " + sourcePf + " does not exist in library " + library);
        }
      }
    }
    // Get specific or all Source PF
    return "SELECT CAST(SYSTEM_TABLE_NAME AS VARCHAR(10) CCSID " + SourceMigrator.INVARIANT_CCSID + ") AS SourcePf " +
        "FROM QSYS2. SYSPARTITIONSTAT " +
        "WHERE SYSTEM_TABLE_SCHEMA = '" + library + "' " +
        (sourcePf.isEmpty() ? "" : "AND SYSTEM_TABLE_NAME = '" + sourcePf + "' ") +
        "AND TRIM(SOURCE_TYPE) <> '' " +
        "GROUP BY SYSTEM_TABLE_NAME, SYSTEM_TABLE_SCHEMA";
  }

}
