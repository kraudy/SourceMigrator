package com.github.kraudy;

import com.github.CliHandler;

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

/**
 * Migrates IBM i source physical files to IFS stream files.
 * @param ifsOutputDirParam IFS output directory (absolute or relative to home). Required in non-interactive mode.
 * @param libraryParam Library name. If null in interactive mode, prompts user.
 * @param sourcePfParam Specific source PF name (optional).
 * @return MigrationResult with outcomes. //TODO: Coming soon.
 * @throws Exception on connection or migration failures.
 */

public class SourceMigrator {
  private static final String UTF8_CCSID = "1208"; // UTF-8 for stream files
  public static final String INVARIANT_CCSID = "37"; // EBCDIC
  private final AS400 system;
  private final Connection connection;
  private final User currentUser;
  private int totalSourcePFsMigrated = 0;
  private int totalMembersMigrated = 0;
  private int migrationErrors = 0;
  private Scanner scanner; // TODO: Remove from here?
  private final boolean interactive;
  private CliHandler cliHandler;

  /*
   * Constructor initializes the AS400 connection and JDBC.
   * 
   * @throws Exception if connection fails
   */
  public SourceMigrator(AS400 system, boolean interactive) throws Exception {
    this(system, new AS400JDBCDataSource(system).getConnection(), interactive);
  }

  public SourceMigrator(AS400 system, Connection connection, boolean interactive) throws Exception {
    this.system = system;

    // Database
    this.connection = connection;
    this.connection.setAutoCommit(true);

    // User
    this.currentUser = new User(system, system.getUserId());
    this.currentUser.loadUserInformation();

    // Input
    this.interactive = interactive;
    this.scanner = interactive ? new Scanner(System.in) : null;
    this.cliHandler = interactive ? new CliHandler(scanner, connection, currentUser): null;
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
      // TODO: homeDir = "/tmp"; // Fallback? for interactive and non?
      if (interactive){
        System.out.println(" *The current user has no home directory.");
        return "";  
      }
      throw new IllegalArgumentException("The current user has no home directory.");
    }
    if (ifsOutputDirParam == null) {
      if (interactive) return cliHandler.promptForOutputDirectory(homeDir); // Prompt for path
      throw new IllegalArgumentException("Output directory required in non-interactive mode");
    }
    if (ifsOutputDirParam.startsWith("/")) {
      return ifsOutputDirParam; // Full path
    }
    return homeDir + "/" + ifsOutputDirParam; // Relative path
  }

  private String getLibrary(String libraryParam) throws IOException, SQLException {
    if (libraryParam == null) {
      if (interactive) return cliHandler.promptForLibrary();
      throw new IllegalArgumentException("Library required in non-interactive mode");
    }
    return validateLibraryNonInteractive(libraryParam);
  }

  private String validateLibraryNonInteractive(String library) throws SQLException {
    try (Statement validateStmt = connection.createStatement();
        ResultSet validateRs = validateStmt.executeQuery(
            "SELECT 1 AS Exists " +
                "FROM QSYS2. SYSPARTITIONSTAT " +
                "WHERE SYSTEM_TABLE_SCHEMA = '" + library + "' LIMIT 1")) {
      if (!validateRs.next()) {
        throw new IllegalArgumentException("Library " + library + " does not exist in your system.");
      }
    }
    return library;
  }

  private String getSourcePFsQuery(String sourcePfParam, String libraryParam, String library)
      throws IOException, SQLException {
    if (sourcePfParam == null){
      if (interactive) return cliHandler.promptForSourcePFs(library);;
      return getSourcePFsNonInteractive("", library); // Get all source pf in library
    }
    return getSourcePFsNonInteractive(sourcePfParam, library); // Get specific source pf in library

  }

  private String getSourcePFsNonInteractive(String sourcePf, String library) throws SQLException {
    if (!sourcePf.isEmpty()) {
      // Validate if Source PF exists
      try (Statement validateStmt = connection.createStatement();
          ResultSet validateRs = validateStmt.executeQuery(
              "SELECT 1 AS Exist FROM QSYS2. SYSPARTITIONSTAT " +
                  "WHERE SYSTEM_TABLE_SCHEMA = '" + library + "' " +
                  "AND SYSTEM_TABLE_NAME = '" + sourcePf + "' " +
                  "AND TRIM(SOURCE_TYPE) <> '' LIMIT 1")) {
        if (!validateRs.next()) {
          throw new IllegalArgumentException("Source PF " + sourcePf + " does not exist in library " + library);
        }
      }
    }
    // Get specific or all Source PF
    return "SELECT CAST(SYSTEM_TABLE_NAME AS VARCHAR(10) CCSID " + INVARIANT_CCSID + ") AS SourcePf " +
        "FROM QSYS2. SYSPARTITIONSTAT " +
        "WHERE SYSTEM_TABLE_SCHEMA = '" + library + "' " +
        (sourcePf.isEmpty() ? "" : "AND SYSTEM_TABLE_NAME = '" + sourcePf + "' ") +
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
            "SELECT CAST(SYSTEM_TABLE_MEMBER AS VARCHAR(10) CCSID " + INVARIANT_CCSID + ") AS Member, " + 
                    "CAST(SOURCE_TYPE AS VARCHAR(10) CCSID " + INVARIANT_CCSID + ") AS SourceType " +
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

  public static void main(String... args) {
    String ifsOutputDirParam = null;
    String libraryParam = null;
    String sourcePfParam = null;
    AS400 system = null;
    SourceMigrator migrator = null;

    try {
      system = IBMiDotEnv.getNewSystemConnection(true);
      
      if (args.length > 0) {
        ifsOutputDirParam = args[0].trim();
        migrator = new SourceMigrator(system, true);
      }
      if (args.length > 1) {
        libraryParam = args[1].trim().toUpperCase();
        migrator = new SourceMigrator(system, false);
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
