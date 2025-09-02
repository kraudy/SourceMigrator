package com.github.kraudy.migrator;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.File;

public class Utilities {
  private final Connection connection;
  private final boolean interactive;

  public Utilities(Connection connection, boolean interactive) {
    this.connection = connection;
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
    //TODO: Validate using SYSTABLES 
    // Get specific or all Source PF
    return "SELECT CAST(SYSTEM_TABLE_NAME AS VARCHAR(10) CCSID " + SourceMigrator.INVARIANT_CCSID + ") AS SourcePf " +
        "FROM QSYS2. SYSPARTITIONSTAT " +
        "WHERE SYSTEM_TABLE_SCHEMA = '" + library + "' " +
        (sourcePf.isEmpty() ? "" : "AND SYSTEM_TABLE_NAME = '" + sourcePf + "' ") +
        "AND TRIM(SOURCE_TYPE) <> '' " +
        "GROUP BY SYSTEM_TABLE_NAME, SYSTEM_TABLE_SCHEMA";
  }

  public String validateAndGetLibrary(String library) throws SQLException {
    try (Statement validateStmt = connection.createStatement();
        ResultSet validateRs = validateStmt.executeQuery(
            "SELECT 1 AS Exists " +
                "FROM QSYS2. SYSPARTITIONSTAT " +
                "WHERE SYSTEM_TABLE_SCHEMA = '" + library + "' LIMIT 1")) {
      if (!validateRs.next()) {
        if (!interactive) throw new IllegalArgumentException("Library " + library + " does not exist in your system.");
        
        System.out.println(" *Library " + library + " does not exist in your system.");
        // Show similar libs
        try (Statement relatedStmt = connection.createStatement();
            ResultSet relatedRs = relatedStmt.executeQuery(
                "SELECT SYSTEM_TABLE_SCHEMA AS library " +
                    "FROM QSYS2. SYSPARTITIONSTAT " +
                    "WHERE SYSTEM_TABLE_SCHEMA LIKE '%" + library + "%' " +
                    "GROUP BY SYSTEM_TABLE_SCHEMA LIMIT 10")) {
          if (relatedRs.next()) {
            System.out.println("Did you mean: ");
            do {
              System.out.println(relatedRs.getString("library").trim());
            } while (relatedRs.next());
          }
        }
        return "";
      }
    }
    return library;
  }

}
