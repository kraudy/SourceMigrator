package com.github.kraudy.migrator;

import com.ibm.as400.access.User;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

public class CliHandler {
  private final Scanner scanner;
  private final Connection connection;
  private final User currentUser;
  private final Utilities utilities;

  public CliHandler(Scanner scanner, Connection connection, User currentUser, Utilities utilities) {
    this.scanner = scanner;
    this.connection = connection;
    this.currentUser = currentUser;
    this.utilities = utilities;
  }

  public String promptForOutputDirectory(String homeDir) throws IOException {
    String defaultDir = homeDir + "/sources";
    String sourceDir = prompt("Specify the source dir destination or press <Enter> to use:", defaultDir);

    if (sourceDir.startsWith("/")) {
      return sourceDir; // Full path
    }

    return sourceDir.isEmpty() ? defaultDir : homeDir + "/" + sourceDir; // Relative path
  }

  public String promptForLibrary() throws IOException, SQLException {
    String library = "";
    while (library.isEmpty()) {
      library = prompt(
          "Specify the name of a library or press <Enter> to search for Source PFs in the current library:",
          currentUser.getCurrentLibraryName());

      if (library.isEmpty()) {
        library = currentUser.getCurrentLibraryName();
        if (library == null || "*CRTDFT".equals(library)) {
          System.out.println("The user does not have a current library");
          library = "";
        }
      } else {
        library = validateAndGetLibrary(library);
      }
    }

    return library;
  }

  private String validateAndGetLibrary(String library) throws SQLException {
    try (Statement validateStmt = connection.createStatement();
        ResultSet validateRs = validateStmt.executeQuery(
            "SELECT 1 AS Exists " +
                "FROM QSYS2. SYSPARTITIONSTAT " +
                "WHERE SYSTEM_TABLE_SCHEMA = '" + library + "' LIMIT 1")) {
      if (!validateRs.next()) {
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

  public String promptForSourcePFs(String library) throws IOException, SQLException {
    showSourcePFs(library); // Show list of source pfs
    String query = "";

    while (query.isEmpty()) {
      String sourcePf = prompt(
          "Specify the name of a source PF or press <Enter> to migrate all the source PFs in library: ",
          "");

      query = utilities.getSourcePFs(sourcePf, library);
    }

    return query;
  }

  private void showSourcePFs(String library) throws SQLException {
    int total = 0;
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(
            "SELECT CAST(SYSTEM_TABLE_NAME AS VARCHAR(10) CCSID " + SourceMigrator.INVARIANT_CCSID + ") AS SourcePf, " +
                "COUNT(*) AS Members " +
                "FROM QSYS2. SYSPARTITIONSTAT " +
                "WHERE SYSTEM_TABLE_SCHEMA = '" + library + "' " +
                "AND TRIM(SOURCE_TYPE) <> '' " +
                "GROUP BY SYSTEM_TABLE_NAME")) {
      System.out.println("\nList of available Source PFs in library: " + library);
      System.out.println("    SourcePf      | Number of Members");
      System.out.println("    ------------- | -----------------");

      while (rs.next()) {
        String sourcePf = rs.getString("SourcePf").trim();
        String membersCount = rs.getString("Members").trim();
        total += Integer.parseInt(membersCount);
        System.out.printf("    %-13s | %17s%n", sourcePf, membersCount);
      }
      System.out.println(String.format("   Total: %27s%n", total));
    }
  }

  private String prompt(String message, String defaultValue) {
    System.out.print(message);

    if (!defaultValue.isEmpty())
      System.out.print(" [" + defaultValue + "]: ");

    String input = this.scanner.nextLine().trim();
    return input.isEmpty() ? defaultValue : input.trim().toUpperCase();
  }
}

