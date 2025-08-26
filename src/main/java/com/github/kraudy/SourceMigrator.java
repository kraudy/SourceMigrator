package com.github.kraudy;

import com.ibm.as400.access.AS400;
import com.ibm.as400.access.AS400SecurityException;
import com.ibm.as400.access.AS400JDBCDataSource;
import com.ibm.as400.access.CommandCall;
import com.ibm.as400.access.ErrorCompletingRequestException;
import com.ibm.as400.access.User;

import io.github.theprez.dotenv_ibmi.IBMiDotEnv;

import java.beans.PropertyVetoException;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/*
  Tool for migrating IBM i source physical files (PFs) to IFS stream files.
*/
public class SourceMigrator {
  private static final String UTF8_CCSID = "1208"; // UTF-8 for stream files
  private static final String INVARIANT_CCSID = "37"; // EBCDIC
  private final AS400 system;
  private final Connection connection;
  private final User currentUser;
  private int totalSourcePFsMigrated = 0;
  private int totalMembersMigrated = 0;
  private int migrationErrors = 0;
  private Scanner scanner;

  /*
   * Constructor initializes the AS400 connection and JDBC.
   * 
   * @throws Exception if connection fails
   */
  public SourceMigrator() throws Exception {
    this.system = IBMiDotEnv.getNewSystemConnection(true);

    // Database
    AS400JDBCDataSource dataSource = new AS400JDBCDataSource(system);
    this.connection = dataSource.getConnection();
    this.connection.setAutoCommit(true);

    // User
    this.currentUser = new User(system, system.getUserId());
    this.currentUser.loadUserInformation();

    // Input
    this.scanner = new Scanner(System.in);
  }

  /* Main entry point of the migration process. */
  public void migrate(String ifsOutputDirParam, String libraryParam, String sourcePfParam) {
    try {
      System.out.println("User: " + system.getUserId().trim().toUpperCase());

      String systemName = getSystemName();
      System.out.println("System: " + systemName);

      String systemCcsid = getCcsid();
      System.out.println("System's CCSID: " + systemCcsid);

      String ifsOutputDir = getOutputDirectory(ifsOutputDirParam);
      if (ifsOutputDir.isEmpty()) {
        return;
      }

      String library = getLibrary(libraryParam);
      if (library.isEmpty()) {
        return;
      }

      ifsOutputDir = ifsOutputDir + "/" + library;
      createDirectory(ifsOutputDir);

      String querySourcePFs = getSourcePFsQuery(sourcePfParam, libraryParam, library);
      if (querySourcePFs.isEmpty()) {
        return;
      }

      long startTime = System.nanoTime();
      migrateSourcePFs(querySourcePFs, ifsOutputDir, library);

      System.out.println("\nMigration completed.");
      System.out.println("Total Source PFs migrated: " + totalSourcePFsMigrated);
      System.out.println("Total members migrated: " + totalMembersMigrated);
      System.out.println("Migration errors: " + migrationErrors);
      long durationNanos = System.nanoTime() - startTime;
      System.out.printf("Total time taken: %.2f seconds%n", TimeUnit.NANOSECONDS.toMillis(durationNanos) / 1000.0);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      cleanup();
    }
  }

  private String getOutputDirectory(String ifsOutputDirParam) throws IOException {
    String homeDir = currentUser.getHomeDirectory(); // Needed for relative path
    if (homeDir == null || homeDir.isEmpty()) {
      System.out.println(" *The current user has no home directory.");
      return "";
    }
    if (ifsOutputDirParam == null) {
      return promptForOutputDirectory(homeDir); // Prompt for path
    }
    if (ifsOutputDirParam.startsWith("/")) {
      return ifsOutputDirParam; // Full path
    }
    return homeDir + "/" + ifsOutputDirParam; // Relative path
  }

  private String promptForOutputDirectory(String homeDir) throws IOException {

    String defaultDir = homeDir + "/sources";
    String sourceDir = prompt("Specify the source dir destination or press <Enter> to use:", defaultDir);

    if (sourceDir.startsWith("/")) {
      return sourceDir; // Full path
    }

    return sourceDir.isEmpty() ? defaultDir : homeDir + "/" + sourceDir; // Relative path
  }

  private String getLibrary(String libraryParam) throws IOException, SQLException {
    if (libraryParam == null) {
      return promptForLibrary();
    }
    return validateAndGetLibrary(libraryParam);
  }

  private String promptForLibrary() throws IOException, SQLException {

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

  private String getSourcePFsQuery(String sourcePfParam, String libraryParam, String library)
      throws IOException, SQLException {
    if (libraryParam != null) {
      return sourcePfParam != null ? getSourcePFs(sourcePfParam, library) : getSourcePFs("", library);
    }
    return promptForSourcePFs(library);
  }

  private String promptForSourcePFs(String library) throws IOException, SQLException {
    showSourcePFs(library); // Show list of source pfs
    String query = "";

    while (query.isEmpty()) {
      String sourcePf = prompt(
          "Specify the name of a source PF or press <Enter> to migrate all the source PFs in library: ",
          "");

      query = getSourcePFs(sourcePf, library);
    }

    return query;
  }

  private String getSourcePFs(String sourcePf, String library) throws SQLException {
    if (!sourcePf.isEmpty()) {
      // Validate if Source PF exists
      try (Statement validateStmt = connection.createStatement();
          ResultSet validateRs = validateStmt.executeQuery(
              "SELECT 1 AS Exist FROM QSYS2. SYSPARTITIONSTAT " +
                  "WHERE SYSTEM_TABLE_SCHEMA = '" + library + "' " +
                  "AND SYSTEM_TABLE_NAME = '" + sourcePf + "' " +
                  "AND TRIM(SOURCE_TYPE) <> '' LIMIT 1")) {
        if (!validateRs.next()) {
          System.out.println(" *Source PF " + sourcePf + " does not exist in library " + library);
          return "";
        }
      }
      // Get specific Source PF
      return "SELECT CAST(SYSTEM_TABLE_NAME AS VARCHAR(10) CCSID " + INVARIANT_CCSID + ") AS SourcePf " +
          "FROM QSYS2. SYSPARTITIONSTAT " +
          "WHERE SYSTEM_TABLE_SCHEMA = '" + library + "' " +
          "AND SYSTEM_TABLE_NAME = '" + sourcePf + "' " +
          "AND TRIM(SOURCE_TYPE) <> '' " +
          "GROUP BY SYSTEM_TABLE_NAME, SYSTEM_TABLE_SCHEMA";
    }
    // Get all Source PF
    return "SELECT CAST(SYSTEM_TABLE_NAME AS VARCHAR(10) CCSID " + INVARIANT_CCSID + ") AS SourcePf " +
        "FROM QSYS2. SYSPARTITIONSTAT " +
        "WHERE SYSTEM_TABLE_SCHEMA = '" + library + "' " +
        "AND TRIM(SOURCE_TYPE) <> '' " +
        "GROUP BY SYSTEM_TABLE_NAME, SYSTEM_TABLE_SCHEMA";
  }

  private void migrateSourcePFs(String querySourcePFs, String baseOutputDir, String library) throws SQLException, IOException,
      AS400SecurityException, ErrorCompletingRequestException, InterruptedException, PropertyVetoException {
    try (Statement stmt = connection.createStatement();
        ResultSet sourcePFs = stmt.executeQuery(querySourcePFs)) {

      while (sourcePFs.next()) {
        String sourcePf = sourcePFs.getString("SourcePf").trim();
        System.out.println("\n\nMigrating Source PF: " + sourcePf + " in library: " + library);

        String pfOutputDir = baseOutputDir + '/' + sourcePf;
        createDirectory(pfOutputDir);

        migrateMembers(library, sourcePf, pfOutputDir);

        totalSourcePFsMigrated++;
      }
    }
  }

  private void migrateMembers(String library, String sourcePf, String ifsOutputDir) throws SQLException, IOException,
      AS400SecurityException, ErrorCompletingRequestException, InterruptedException, PropertyVetoException {
    try (Statement stmt = connection.createStatement();
        ResultSet rsMembers = stmt.executeQuery(
            "SELECT SYSTEM_TABLE_MEMBER AS Member, SOURCE_TYPE AS SourceType " +
                "FROM QSYS2. SYSPARTITIONSTAT " +
                "WHERE SYSTEM_TABLE_SCHEMA = '" + library + "' " +
                "AND SYSTEM_TABLE_NAME = '" + sourcePf + "' " +
                "AND TRIM(SOURCE_TYPE) <> ''")) {

      List<CompletableFuture<Void>> futures = new ArrayList<>();
      while (rsMembers.next()) {
        String memberName = rsMembers.getString("Member").trim();
        String sourceType = rsMembers.getString("SourceType").trim();

        CompletableFuture<Void> future = migrateMemberAsync(library, sourcePf, memberName, sourceType, ifsOutputDir);
        futures.add(future);
      }

      CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
      allFutures.join();
    }
  }

  private CompletableFuture<Void> migrateMemberAsync(String library, String sourcePf, String memberName,
      String sourceType, String ifsOutputDir) {
    return CompletableFuture.runAsync(() -> {
      try {
        String commandStr = "CPYTOSTMF FROMMBR('/QSYS.lib/" + library + ".lib/" + sourcePf + ".file/" + memberName
            + ".mbr') " +
            "TOSTMF('" + ifsOutputDir + "/" + memberName + "." + sourceType + "') " +
            "STMFOPT(*REPLACE) STMFCCSID(" + UTF8_CCSID + ") ENDLINFMT(*LF)";

        CommandCall cmd = new CommandCall(system);
        if (!cmd.run(commandStr)) {
          System.out.println("Could not migrate " + memberName + ": Failed");
          migrationErrors++;
        } else {
          System.out.println("Migrated " + memberName + ": OK");
          totalMembersMigrated++;
        }

      } catch (AS400SecurityException | ErrorCompletingRequestException | IOException | InterruptedException
          | PropertyVetoException e) {

        System.out.println("Could not migrate " + memberName + ": Failed");
        migrationErrors++;
        e.printStackTrace();
      }

    });
  }

  private String getCcsid() throws SQLException {
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

  private String getSystemName() throws SQLException {
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT CURRENT_SERVER AS Server FROM SYSIBM. SYSDUMMY1")) {
      if (rs.next()) {
        return rs.getString("Server").trim();
      }
    }
    return "UNKNOWN";
  }

  private void showSourcePFs(String library) throws SQLException {
    int total = 0;
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(
            "SELECT CAST(SYSTEM_TABLE_NAME AS VARCHAR(10) CCSID " + INVARIANT_CCSID + ") AS SourcePf, " +
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

  private void createDirectory(String dirPath) {
    File outputDir = new File(dirPath);
    if (!outputDir.exists()) {
      System.out.println("Creating dir: " + dirPath + " ...");
      outputDir.mkdirs();
    }
  }

  private void cleanup() {
    try {
      if (connection != null && !connection.isClosed()) {
        connection.close();
      }
      if (system != null) {
        system.disconnectAllServices();
      }

      if (scanner != null) {
        scanner.close();
      }

    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  private String prompt(String message, String defaultValue) {
    System.out.print(message);

    if (!defaultValue.isEmpty())
      System.out.print(" [" + defaultValue + "]: ");

    String input = this.scanner.nextLine().trim();
    return input.isEmpty() ? defaultValue : input.trim().toUpperCase();
  }

  public static void main(String... args) {
    String ifsOutputDirParam = null;
    String libraryParam = null;
    String sourcePfParam = null;

    try {
      SourceMigrator migrator = new SourceMigrator();

      if (args.length > 0) {
        ifsOutputDirParam = args[0].trim();
      }
      if (args.length > 1) {
        libraryParam = args[1].trim().toUpperCase();
      }
      if (args.length > 2) {
        sourcePfParam = args[2].trim().toUpperCase();
      }

      migrator.migrate(ifsOutputDirParam, libraryParam, sourcePfParam);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
