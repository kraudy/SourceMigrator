package com.github.kraudy.migrator;

import com.ibm.as400.access.AS400;
import com.ibm.as400.access.AS400SecurityException;
import com.ibm.as400.access.AS400JDBCDataSource;
import com.ibm.as400.access.CommandCall;
import com.ibm.as400.access.ErrorCompletingRequestException;
import com.ibm.as400.access.User;

import io.github.theprez.dotenv_ibmi.IBMiDotEnv;

import java.beans.PropertyVetoException;
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

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Migrates IBM i source physical files to IFS stream files.
 * @param ifsOutputDirParam IFS output directory (absolute or relative to home). Required in non-interactive mode.
 * @param libraryParam Library name. If null in interactive mode, prompts user.
 * @param sourcePfParam Specific source PF name (optional).
 * @return MigrationResult with outcomes. //TODO: Coming soon.
 * @throws Exception on connection or migration failures.
 */

@Command(name = "migrator", description = "Migrates IBM i source physical files to IFS stream files.", mixinStandardHelpOptions = true)
public class SourceMigrator implements Runnable{
  private static final String UTF8_CCSID = "1208"; // UTF-8 for stream files
  public static final String INVARIANT_CCSID = "37"; // EBCDIC
  private final AS400 system;
  private final Connection connection;
  private final User currentUser;
  private int totalSourcePFsMigrated = 0;
  private int totalMembersMigrated = 0;
  private int migrationErrors = 0;
  private Scanner scanner; // TODO: Remove from here?
  private boolean interactive; //TODO: Is removing final a good idea? I would preffer it to be created in the constructor
  private CliHandler cliHandler;
  private Utilities utilities;

  static class outDirConverter implements CommandLine.ITypeConverter<String> {
    @Override
    public String convert(String outDir) throws Exception {
      try {
        return outDir.trim();
      } catch (IllegalArgumentException e) {
        throw new CommandLine.TypeConversionException("Invalid object name: '" + outDir);
      }
    }
  }

  @Option(names = { "-l", "--libs" }, required = true, arity = "1..*", description = "Library list to scan")
  private List<String> libraryList = new ArrayList<>();

  @Option(names = "-o", description = "Sources destination", converter = outDirConverter.class)
  private String outDir = "sources";

  @Option(names = "--pf", description = "Source Phisical File")
  private String sourcePf = null;

  @Parameters(arity = "0..*", description = "<output_dir> <library> [source_pf]", paramLabel = "<args>")
  private List<String> parameters;

  @Option(names = "-x", description = "Debug")
  private boolean debug = false;

  @Option(names = "-v", description = "Verbose output")
  private boolean verbose = false;

  @Option(names = "--json", description = "Output as JSON")
  private boolean jsonOutput = false;

  @Option(names = { "-h", "--help" }, usageHelp = true, description = "Builds dependency graph for IBM i objects")
  private boolean helpRequested = false;

  /*
   * Constructor initializes the AS400 connection and JDBC.
   * 
   * @throws Exception if connection fails
   */

   public SourceMigrator(AS400 system) throws Exception {
    this(system, new AS400JDBCDataSource(system).getConnection());
  }

  public SourceMigrator(AS400 system, Connection connection) throws Exception {
    this.system = system;

    // Database
    this.connection = connection;
    this.connection.setAutoCommit(true);

    // User
    this.currentUser = new User(system, system.getUserId());
    this.currentUser.loadUserInformation();
  }

  public SourceMigrator(AS400 system, boolean interactive) throws Exception {
    this(system);
    initInteractive(interactive);
  }

  public SourceMigrator(AS400 system, Connection connection, boolean interactive) throws Exception {
    this(system, connection);
    initInteractive(interactive);
  }

  private void initInteractive(boolean interactive) {
    this.interactive = interactive;
    this.utilities = new Utilities(connection, interactive);
    this.scanner = interactive ? new Scanner(System.in) : null;
    this.cliHandler = interactive ? new CliHandler(scanner, connection, currentUser, utilities) : null;
  }

  @Override
  public void run() {
    try {
      outDir = getOutputDirectory(outDir); // Validate source dir
      boolean calculatedInteractive = (parameters == null || parameters.size() <= 1);
      initInteractive(calculatedInteractive);

      String libraryParam = (parameters != null && parameters.size() > 1) ? parameters.get(1).trim().toUpperCase() : null;
      String sourcePfParam = (parameters != null && parameters.size() > 2) ? parameters.get(2).trim().toUpperCase() : null;

      migrate(outDir, libraryParam, sourcePfParam);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      cleanup();
    }
  }

  /* Main entry point of the migration process. */
  public void migrate(String ifsOutputDir, String libraryParam, String sourcePfParam) {
    try {
      System.out.println("User: " + system.getUserId().trim().toUpperCase());

      String systemName = utilities.getSystemName();
      System.out.println("System: " + systemName);

      String systemCcsid = utilities.getCcsid();
      System.out.println("System's CCSID: " + systemCcsid);


      String library = getLibrary(libraryParam);
      if (library.isEmpty()) {
        return;
      }

      ifsOutputDir = ifsOutputDir + "/" + library;
      utilities.createDirectory(ifsOutputDir);

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

  private String getOutputDirectory(String outDir) throws IOException {
    if (outDir.startsWith("/")) {
      return outDir; // Full path
    }

    String homeDir = currentUser.getHomeDirectory(); // Needed for relative path
    if (homeDir == null || homeDir.isEmpty()) {
      homeDir = "/tmp"; // Fallback
      if (verbose) System.out.println(" *The current user has no home directory.");
      //TODO: Add param for this, like something to make it crash, maybe -x? or some default params
      //throw new IllegalArgumentException("The current user has no home directory.");
    }

    return homeDir + "/" + outDir; // Relative path
  }

  private String getLibrary(String libraryParam) throws IOException, SQLException {
    if (libraryParam == null) {
      if (interactive) return cliHandler.promptForLibrary();
      throw new IllegalArgumentException("Library required in non-interactive mode");
    }
    return utilities.validateAndGetLibrary(libraryParam);
  }

  private String getSourcePFsQuery(String sourcePfParam, String libraryParam, String library)
      throws IOException, SQLException {
    if (sourcePfParam == null){
      if (interactive) return cliHandler.promptForSourcePFs(library);;
      return utilities.getSourcePFs("", library); // Get all source pf in library
    }
    return utilities.getSourcePFs(sourcePfParam, library); // Get specific source pf in library

  }

  private void migrateSourcePFs(String querySourcePFs, String baseOutputDir, String library) throws SQLException, IOException,
      AS400SecurityException, ErrorCompletingRequestException, InterruptedException, PropertyVetoException {
    try (Statement stmt = connection.createStatement();
        ResultSet sourcePFs = stmt.executeQuery(querySourcePFs)) {

      while (sourcePFs.next()) {
        String sourcePf = sourcePFs.getString("SourcePf").trim();
        System.out.println("\n\nMigrating Source PF: " + sourcePf + " in library: " + library);

        String pfOutputDir = baseOutputDir + '/' + sourcePf;
        utilities.createDirectory(pfOutputDir);

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
    AS400 system = null;
    SourceMigrator migrator = null;

    try {
      system = IBMiDotEnv.getNewSystemConnection(true); // Get system
      
      migrator = new SourceMigrator(system);
      new CommandLine(migrator).execute(args);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
