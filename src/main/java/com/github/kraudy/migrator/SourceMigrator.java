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
import java.util.Arrays;
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

  private List<String> migratedPaths;

  private boolean returnPaths = false;

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

  public SourceMigrator(AS400 system, Connection connection, String library, String sourcePf, List<String> members, 
      String outDir, boolean debug, boolean verbose) throws Exception {

    this(system, connection);

    this.library = library;
    this.sourcePf = sourcePf;
    this.members = members;
    this.outDir = outDir;
    this.debug = debug;
    this.verbose = verbose;

  }

  @Override
  public void run() {
    try {
      api();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      cleanup();
    }
  }

  public void setParams(String library, String sourcePf, List<String> members, 
      String outDir, boolean debug, boolean verbose){
    this.library = library;
    this.sourcePf = sourcePf;
    this.members = members;
    this.outDir = outDir;
    this.debug = debug;
    this.verbose = verbose;

    this.returnPaths = true;
  }

  public void api(){
    try {
      // Utilities
      this.utilities = new Utilities(connection, currentUser, verbose);

      outDir = utilities.getOutputDirectory(outDir); // Validate source dir
      
      utilities.validateLibrary(library);
      utilities.createDirectory(outDir + "/" + library);

      if (!members.isEmpty() && sourcePf.isEmpty()) {
        throw new IllegalArgumentException("Members can only be specified when a specific source PF is provided.");
      }
      
      /* No specific sourcPf nor Members */
      if(sourcePf.isEmpty() && members.isEmpty()){
        utilities.createDirectory(outDir, library);
      }

      /* Specific SourcPf and no Members */
      if(!sourcePf.isEmpty() && members.isEmpty()){
        utilities.validateSourcePFs(sourcePf, library);
        utilities.createDirectory(outDir, library, sourcePf);
      }

      /* Specific SourcPf and Members */
      if (!sourcePf.isEmpty() && !members.isEmpty()) {
        utilities.validateSourcePFs(sourcePf, library);
        utilities.createDirectory(outDir, library, sourcePf);
        members = members.stream().map(String::trim).map(String::toUpperCase).distinct().collect(Collectors.toList());
        utilities.validateMembers(library, sourcePf, members);
      }

      String querySources = utilities.getMigrationQuery(library, sourcePf, members);

      //TODO: Add verbose validation
      System.out.println("User: " + system.getUserId().trim().toUpperCase());
      System.out.println("System: " + utilities.getSystemName());
      System.out.println("System's CCSID: " + utilities.getCcsid());

      long startTime = System.nanoTime();

      migrate(querySources, outDir + "/" + library, library);

      System.out.println("\nMigration completed.");
      System.out.println("Total Source PFs migrated: " + totalSourcePFsMigrated);
      System.out.println("Total members migrated: " + totalMembersMigrated);
      System.out.println("Migration errors: " + migrationErrors);
      long durationNanos = System.nanoTime() - startTime;
      System.out.printf("Total time taken: %.2f seconds%n", TimeUnit.NANOSECONDS.toMillis(durationNanos) / 1000.0);
      
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public List<String> getPathList(){
    if (returnPaths){
       return migratedPaths;
    }
    return Arrays.asList("");
  }

  public String getPathList(int index){
    if (returnPaths){
       return migratedPaths.get(index);
    }
    return "";
  }

  public String getFirstPath(){
    return getPathList(0);
  }

  /* Main entry point of the migration process. */
  public void migrate(String querySources, String ifsOutputDir, String library) throws SQLException, IOException,
      AS400SecurityException, ErrorCompletingRequestException, InterruptedException, PropertyVetoException {
    try (Statement stmt = connection.createStatement();
        ResultSet rsQuerySources = stmt.executeQuery(querySources)) {

      List<CompletableFuture<Void>> futures = new ArrayList<>();
      while (rsQuerySources.next()) {
        String sourcePf = rsQuerySources.getString("SourcePf").trim();
        String memberName = rsQuerySources.getString("Member").trim();
        String sourceType = rsQuerySources.getString("SourceType").trim();

        CompletableFuture<Void> future = migrateAsync(library, sourcePf, memberName, sourceType, ifsOutputDir + "/" + sourcePf);
        futures.add(future);

        // TODO: Adjust this count
        //totalSourcePFsMigrated++;
      }

      CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
      allFutures.join();
    }
  }

  private CompletableFuture<Void> migrateAsync(String library, String sourcePf, String memberName,
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
          System.out.println("Migrated SourcePf: " + sourcePf + " | member: " + memberName + "." + sourceType + ": OK");
          totalMembersMigrated++;
          if (returnPaths){
            migratedPaths.add("'" + ifsOutputDir + "/" + memberName + "." + sourceType + "'");
          }
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
