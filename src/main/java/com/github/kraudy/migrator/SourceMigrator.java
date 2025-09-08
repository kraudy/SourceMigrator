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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
  private Utilities utilities;

  static class OutDirConverter implements CommandLine.ITypeConverter<String> {
    @Override
    public String convert(String outDir) throws Exception {
      try {
        return outDir.trim();
      } catch (IllegalArgumentException e) {
        throw new CommandLine.TypeConversionException("Invalid object name: '" + outDir);
      }
    }
  }

  static class LibraryConverter implements CommandLine.ITypeConverter<String> {
    @Override
    public String convert(String library) throws Exception {
      try {
        return library.trim().toUpperCase();
      } catch (IllegalArgumentException e) {
        throw new CommandLine.TypeConversionException("Invalid library name: '" + library);
      }
    }
  }

  static class SourcePfConverter implements CommandLine.ITypeConverter<String> {
    @Override
    public String convert(String sourcePf) throws Exception {
      try {
        return sourcePf.trim().toUpperCase();
      } catch (IllegalArgumentException e) {
        throw new CommandLine.TypeConversionException("Invalid source PF: '" + sourcePf);
      }
    }
  }

  static class SourceMemberConverter implements CommandLine.ITypeConverter<String> {
    @Override
    public String convert(String sourceMember) throws Exception {
      try {
        return sourceMember.trim().toUpperCase();
      } catch (IllegalArgumentException e) {
        throw new CommandLine.TypeConversionException("Invalid source member: '" + sourceMember);
      }
    }
  }

  @Option(names = { "-sl", "--source-lib" }, required = true, description = "Source library", converter = LibraryConverter.class)
  private String library;

  @Option(names = "--spf", description = "Source Physical File", converter = SourcePfConverter.class)
  private String sourcePf = "";

  @Option(names = "--mbrs", arity = "0..*", description = "Specific source members to migrate", converter = SourceMemberConverter.class)
  private List<String> members = new ArrayList<>();

  /* 
  @Option(names = {"-ut", "--updated-time"}, description = "Migrate only sources with change after timestamp")
  private Timestamp updateTime;

  @Option(names = {"-ct", "--creation-time"}, description = "Migrate only sources created after timestamp")
  private Timestamp updateTime;
  */
   
  @Option(names = "-o", description = "Sources destination", converter = OutDirConverter.class)
  private String outDir = "sources";

  @Option(names = "-x", description = "Debug")
  private boolean debug = false;

  @Option(names = "-v", description = "Verbose output")
  private boolean verbose = false;

  @Option(names = "--json", description = "Output as JSON")
  private boolean jsonOutput = false;

  @Option(names = { "-h", "--help" }, usageHelp = true, description = "Migrates IBM i source physical files to IFS stream files")
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

  @Override
  public void run() {
    try {
      // Utilities
      this.utilities = new Utilities(connection, currentUser, verbose);

      outDir = utilities.getOutputDirectory(outDir); // Validate source dir
      
      utilities.validateLibrary(library);
      utilities.createDirectory(outDir + "/" + library);

      if(!sourcePf.isEmpty()){
        utilities.validateSourcePFs(sourcePf, library);
      }

      if (!members.isEmpty() && sourcePf.isEmpty()) {
        throw new IllegalArgumentException("Members can only be specified when a specific source PF is provided.");
      }

      if (!members.isEmpty()) {
        members = members.stream().map(String::trim).map(String::toUpperCase).distinct().collect(Collectors.toList());
        utilities.validateMembers(library, sourcePf, members);
      }

      //TODO: Add verbose validation
      System.out.println("User: " + system.getUserId().trim().toUpperCase());
      System.out.println("System: " + utilities.getSystemName());
      System.out.println("System's CCSID: " + utilities.getCcsid());

      long startTime = System.nanoTime();

      String querySourcePFs = null;
      if (sourcePf.isEmpty()) querySourcePFs = utilities.getMigrationQuery(library); // Get all source pf 
      if (!sourcePf.isEmpty() && members.isEmpty()) querySourcePFs = utilities.getMigrationQuery(library, sourcePf); // Get specific source pf
      if (!sourcePf.isEmpty() && !members.isEmpty()) querySourcePFs = utilities.getMigrationQuery(library, sourcePf, members); // Get specific source members

      // TODO: This could be colled once for every library in the list
      migrate(querySourcePFs, outDir + "/" + library, library);

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

  /* Main entry point of the migration process. */
  public void migrate(String querySourcePFs, String ifsOutputDir, String library) throws SQLException, IOException,
      AS400SecurityException, ErrorCompletingRequestException, InterruptedException, PropertyVetoException {
    try (Statement stmt = connection.createStatement();
        ResultSet sourcePFs = stmt.executeQuery(querySourcePFs)) {

      while (sourcePFs.next()) {
        String sourcePf = sourcePFs.getString("SourcePf").trim();
        System.out.println("\n\nMigrating Source PF: " + sourcePf + " in library: " + library);

        // TODO: Should i create this when validating the source pf like i did with the libs?
        String pfOutputDir = ifsOutputDir + '/' + sourcePf;
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
        //TODO: Should i use cmdStmt.execute instead of this?
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
