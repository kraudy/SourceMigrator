package com.github.kraudy;
import com.ibm.as400.access.AS400;
import com.ibm.as400.access.AS400SecurityException;
import com.ibm.as400.access.AS400JDBCDataSource;
import com.ibm.as400.access.CommandCall;
import com.ibm.as400.access.ErrorCompletingRequestException;
import com.ibm.as400.access.User;
import java.beans.PropertyVetoException;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

/*
  Tool for migrating IBM i source physical files (PFs) to IFS stream files.
*/
public class SourceMigrator { 
  private static final String UTF8_CCSID = "1208";    // UTF-8 for stream files
  private static final String INVARIANT_CCSID = "37"; // EBCDIC
  private final AS400 system;
  private final Connection connection;
  private final BufferedReader inputReader;
  private final User currentUser; 
  private int totalSourcePFsMigrated = 0;
  private int totalMembersMigrated = 0;
  private int migrationErrors = 0; 

  /*
  Constructor initializes the AS400 connection and JDBC.
  @throws Exception if connection fails
  */
  public SourceMigrator() throws Exception {
    this.system = new AS400();
    this.currentUser = new User(system, system.getUserId());
    this.inputReader = new BufferedReader(new InputStreamReader(System.in)); 
    AS400JDBCDataSource dataSource = new AS400JDBCDataSource(system);
    this.connection = dataSource.getConnection();
    this.connection.setAutoCommit(true);
  } 
  /* Main entry point of the migration process.*/
  public void migrate(String ifsOutputDirParam, String libraryParam, String sourcePfParam) {
    try {
      System.out.println("User: " + system.getUserId().trim().toUpperCase());

      String systemName = getSystemName();
      System.out.println("System: " + systemName);

      String systemCcsid = getCcsid();
      System.out.println("System's CCSID: " + systemCcsid);

      String homeDir = currentUser.getHomeDirectory();
      if (homeDir == null || homeDir.isEmpty()) {
        System.out.println("The current user has no home directory.");
        return;
      }

      String ifsOutputDir = getOutputDirectory(ifsOutputDirParam, homeDir);
      if (ifsOutputDir.isEmpty()) {
        return;
      }
      
      String library = getLibrary(libraryParam);
      if (library.isEmpty()) {
        return;
      }

      ifsOutputDir = ifsOutputDir + "/" + library;
      createDirectory(ifsOutputDir);

      ResultSet sourcePFs = getSourcePFsResultSet(sourcePfParam, libraryParam, library);
      if (sourcePFs == null) {
          return;
      }

      long startTime = System.nanoTime();
      migrateSourcePFs(sourcePFs, ifsOutputDir);

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
  private String getOutputDirectory(String ifsOutputDirParam, String homeDir) throws IOException {
    if (ifsOutputDirParam == null){
      return promptForOutputDirectory(homeDir); // Prompt for path
    }
    if (ifsOutputDirParam.startsWith("/")) { 
      return ifsOutputDirParam; // Full path
    } 
    return homeDir + "/" + ifsOutputDirParam; //Relative path
  }
  private String promptForOutputDirectory(String homeDir) throws IOException {
    String defaultDir = homeDir + "/sources";
    System.out.println("\nSpecify the source dir destination or press 'Enter' to use: " + defaultDir);
    String sourceDir = inputReader.readLine().trim();
    if(sourceDir.startsWith("/")){
      return sourceDir; // Full path
    }
    return sourceDir.isEmpty() ? defaultDir : homeDir + "/" + sourceDir; //Relative path
  } 
  private String getLibrary(String libraryParam) throws IOException, SQLException {
    if (libraryParam == null){
      return promptForLibrary();
    } 
    return validateAndGetLibrary(libraryParam);
  }
  private String promptForLibrary() throws IOException, SQLException {
    String library = "";
    while (library.isEmpty()) {
      System.out.println("\nSpecify the name of a library or press enter to search for Source PFs in the current library: " + currentUser.getCurrentLibraryName());
      library = inputReader.readLine().trim().toUpperCase();     
      if (library.isEmpty()) {
        library = currentUser.getCurrentLibraryName();
        if ("*CRTDFT".equals(library)) {
          System.out.println("The user does not have a current library");
          library = "";
          continue;
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
                        "FROM QSYS2.SYSPARTITIONSTAT " +
                        "WHERE SYSTEM_TABLE_SCHEMA = '" + library + "' LIMIT 1")) {     
      if (!validateRs.next()) {
        System.out.println(" *Library " + library + " does not exist in your system.");
        // Show similar libs
        try (Statement relatedStmt = connection.createStatement();
              ResultSet relatedRs = relatedStmt.executeQuery(
                      "SELECT SYSTEM_TABLE_SCHEMA AS library " +
                              "FROM QSYS2.SYSPARTITIONSTAT " +
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
  private ResultSet getSourcePFsResultSet(String sourcePfParam, String libraryParam, String library) throws IOException, SQLException{
    if (sourcePfParam != null) {
      return getSourcePFs(sourcePfParam, library);
    }
    if (libraryParam != null) {
      return getSourcePFs("", library);
    }
    return promptForSourcePFs(library);
  } 
  private ResultSet promptForSourcePFs(String library) throws IOException, SQLException {
    showSourcePFs(library);
    ResultSet sourcePFs = null;
    while (sourcePFs == null) {
      System.out.println("\nSpecify the name of a source PF or press 'Enter' to migrate all the source PFs in library: " + library);
      String sourcePf = inputReader.readLine().trim().toUpperCase();
      sourcePFs = getSourcePFs(sourcePf, library);
    }
    return sourcePFs;
  } 
  private void migrateSourcePFs(ResultSet sourcePFs, String baseOutputDir) throws SQLException, IOException, AS400SecurityException, ErrorCompletingRequestException, InterruptedException, PropertyVetoException {
    while (sourcePFs.next()) {
      String library = sourcePFs.getString("Library").trim();
      String sourcePf = sourcePFs.getString("SourcePf").trim();     
      System.out.println("\n\nMigrating Source PF: " + sourcePf + " in library: " + library);

      String pfOutputDir = baseOutputDir + '/' + sourcePf;
      createDirectory(pfOutputDir);

      migrateMembers(library, sourcePf, pfOutputDir);

      totalSourcePFsMigrated++;
    }
    sourcePFs.close(); 
  } 
  private void migrateMembers(String library, String sourcePf, String ifsOutputDir) throws SQLException, IOException, AS400SecurityException, ErrorCompletingRequestException, InterruptedException, PropertyVetoException {
    try (Statement stmt = connection.createStatement();
          ResultSet rsMembers = stmt.executeQuery(
                  "SELECT SYSTEM_TABLE_MEMBER AS Member, SOURCE_TYPE AS SourceType " +
                          "FROM QSYS2.SYSPARTITIONSTAT " +
                          "WHERE SYSTEM_TABLE_SCHEMA = '" + library + "' " +
                          "AND SYSTEM_TABLE_NAME = '" + sourcePf + "' " +
                          "AND TRIM(SOURCE_TYPE) <> ''")){ 
        while (rsMembers.next()) {
          String memberName = rsMembers.getString("Member").trim();
          String sourceType = rsMembers.getString("SourceType").trim();
          System.out.println("\n=== Processing Member: " + memberName + " ===");

          if (!migrateMemberUsingCommand(library, sourcePf, memberName, sourceType, ifsOutputDir)) {
              migrationErrors++;
          } else {
              totalMembersMigrated++;
          }
        }
    } 
  } 
  private boolean migrateMemberUsingCommand(String library, String sourcePf, String memberName, String sourceType, String ifsOutputDir) throws IOException, AS400SecurityException, ErrorCompletingRequestException, InterruptedException, PropertyVetoException {
    String commandStr = "CPYTOSTMF FROMMBR('/QSYS.lib/" + library + ".lib/" + sourcePf + ".file/" + memberName + ".mbr') " +
              "TOSTMF('" + ifsOutputDir + "/" + memberName + "." + sourceType + "') " +
              "STMFOPT(*REPLACE) STMFCCSID(" + UTF8_CCSID + ") ENDLINFMT(*LF)"; 

    CommandCall cmd = new CommandCall(system);
    if (!cmd.run(commandStr)) {
      System.out.println("Could not migrate " + memberName + " ***");
      return false;
    }

    System.out.println("Migrated " + memberName + " successfully");
    return true; 
  } 
  
  private String getCcsid() throws SQLException {
    try (Statement stmt = connection.createStatement();
      ResultSet rs = stmt.executeQuery(
                  "Select CCSID From QSYS2.SYSCOLUMNS WHERE TABLE_NAME = 'SYSPARTITIONSTAT'" + 
                  "And TABLE_SCHEMA = 'QSYS2' And COLUMN_NAME = 'SYSTEM_TABLE_NAME' "
      )) {
      if (rs.next()) {
        return rs.getString("CCSID").trim();
      }
    }
    return "";
  } 
  
  private String getSystemName() throws SQLException {
    try (Statement stmt = connection.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT CURRENT_SERVER AS Server FROM SYSIBM.SYSDUMMY1")) {
      if (rs.next()) {
        return rs.getString("Server").trim();
      }
    }
    return "UNKNOWN";
  } 
  private void showSourcePFs(String library) throws SQLException {
    try (Statement stmt = connection.createStatement();
          ResultSet rs = stmt.executeQuery(
                  "SELECT CAST(SYSTEM_TABLE_NAME AS VARCHAR(10) CCSID " + INVARIANT_CCSID + ") AS SourcePf, " +
                          "COUNT(*) AS Members " +
                  "FROM QSYS2.SYSPARTITIONSTAT " +
                  "WHERE SYSTEM_TABLE_SCHEMA = '" + library + "' " +
                  "AND TRIM(SOURCE_TYPE) <> '' " +
                  "GROUP BY SYSTEM_TABLE_NAME")) {     
      System.out.println("\nList of available Source PFs in library: " + library);
      System.out.println("    SourcePf      | Number of Members");
      System.out.println("    ------------- | -----------------");

      while (rs.next()) {
        String sourcePf = rs.getString("SourcePf").trim();
        String membersCount = rs.getString("Members").trim();
        System.out.printf("    %-13s | %17s%n", sourcePf, membersCount);
      }
    } 
  } 
  private ResultSet getSourcePFs(String sourcePf, String library) throws SQLException {
    String query;
    if (!sourcePf.isEmpty()) {
      // Validate if Source PF exists
      try (Statement validateStmt = connection.createStatement();
          ResultSet validateRs = validateStmt.executeQuery(
                  "SELECT 1 AS Exist FROM QSYS2.SYSPARTITIONSTAT " +
                          "WHERE SYSTEM_TABLE_SCHEMA = '" + library + "' " +
                          "AND SYSTEM_TABLE_NAME = '" + sourcePf + "' " +
                          "AND TRIM(SOURCE_TYPE) <> '' LIMIT 1")) {         
        if (!validateRs.next()) {
          System.out.println(" *Source PF " + sourcePf + " does not exist in library " + library);
          return null;
        }
      }
      // Get specific Source PF
      query = "SELECT CAST(SYSTEM_TABLE_NAME AS VARCHAR(10) CCSID " + INVARIANT_CCSID + ") AS SourcePf, " +
              "SYSTEM_TABLE_SCHEMA AS Library " +
              "FROM QSYS2.SYSPARTITIONSTAT " +
              "WHERE SYSTEM_TABLE_SCHEMA = '" + library + "' " +
              "AND SYSTEM_TABLE_NAME = '" + sourcePf + "' " +
              "AND TRIM(SOURCE_TYPE) <> '' " +
              "GROUP BY SYSTEM_TABLE_NAME, SYSTEM_TABLE_SCHEMA";
    } else {
      // Get all Source PF
      query = "SELECT CAST(SYSTEM_TABLE_NAME AS VARCHAR(10) CCSID " + INVARIANT_CCSID + ") AS SourcePf, " +
              "SYSTEM_TABLE_SCHEMA AS Library " +
              "FROM QSYS2.SYSPARTITIONSTAT " +
              "WHERE SYSTEM_TABLE_SCHEMA = '" + library + "' " +
              "AND TRIM(SOURCE_TYPE) <> '' " +
              "GROUP BY SYSTEM_TABLE_NAME, SYSTEM_TABLE_SCHEMA";
    }

    Statement stmt = connection.createStatement();
    return stmt.executeQuery(query); 
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
    } catch (SQLException e) {
      e.printStackTrace();
    }
  } 
  public static void main(String... args) {
    try {
      SourceMigrator migrator = new SourceMigrator();
      String ifsOutputDirParam = null;
      String libraryParam = null;
      String sourcePfParam = null;
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

