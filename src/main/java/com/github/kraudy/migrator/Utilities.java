package com.github.kraudy.migrator;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.ibm.as400.access.User;

import java.io.File;
import java.io.IOException;

public class Utilities {
  private final Connection connection;
  private final boolean verbose;
  private final User currentUser;

  public Utilities(Connection connection, User currentUser, boolean verbose) {
    this.connection = connection;
    this.currentUser = currentUser;
    this.verbose = verbose;
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

  //TODO: Maybe this should return the FILE object
  public String getIFSPath(String outDir) throws IOException {
    if (outDir.startsWith("/")) {
      return outDir; // Full path
    }

    String homeDir = currentUser.getHomeDirectory(); // Needed for relative path
    if (homeDir == null || homeDir.isEmpty()) {
      homeDir = "/tmp"; // Fallback
      if (verbose) System.err.println(" *The current user has no home directory. Default to '/tmp'");
      //TODO: Add param for this, like something to make it crash, maybe -x? or some default params
      //throw new IllegalArgumentException("The current user has no home directory.");
    }

    return homeDir + "/" + outDir; // Relative path
  }

  //TODO: Change to validateIFSPath and create another for createDirectory
  public boolean validateIFSPath(String path){
    File sourceFile = new File(path);
    return sourceFile.exists();
  }

  public void createDirectory(String dirPath) {
    //TODO: Change to jt400 IFSFile
    File outputDir = new File(dirPath);
    if (outputDir.exists()) {
      if (verbose) System.err.println(" *Dir already exists: " + dirPath + " ...");
      return;
    }
    if (verbose) System.out.println("Creating dir: " + dirPath + " ...");
    outputDir.mkdirs();
  }

  public void createDirectory(String dirPath, String library) throws SQLException {
    try (Statement stmt = connection.createStatement();
        ResultSet sourcePFs = stmt.executeQuery(
          "SELECT CAST(SYSTEM_TABLE_NAME AS VARCHAR(10) CCSID " + SourceMigrator.INVARIANT_CCSID + ") AS SourcePf " +
          "FROM QSYS2. SYSPARTITIONSTAT " +
          "WHERE SYSTEM_TABLE_SCHEMA = '" + library + "' " +
          "AND TRIM(SOURCE_TYPE) <> '' " +
          "GROUP BY SYSTEM_TABLE_NAME, SYSTEM_TABLE_SCHEMA"
        )) {
      while (sourcePFs.next()) {
        String sourcePf = sourcePFs.getString("SourcePf").trim();

        createDirectory(dirPath, library, sourcePf);

      }
    }
    
  }

  public void createDirectory(String dirPath, String library, String sourcePf) {
    createDirectory(dirPath + "/" + library + "/" + sourcePf);
  }

  //TODO: Add timestamp validation.
  public String getMigrationQuery(String library, String sourcePf, List<String> members) throws SQLException {
    return "SELECT CAST(SYSTEM_TABLE_NAME AS VARCHAR(10) CCSID " + SourceMigrator.INVARIANT_CCSID + ") AS SourcePf, " +
                  "CAST(SYSTEM_TABLE_MEMBER AS VARCHAR(10) CCSID " + SourceMigrator.INVARIANT_CCSID + ") AS Member, " + 
                  "CAST(SOURCE_TYPE AS VARCHAR(10) CCSID " + SourceMigrator.INVARIANT_CCSID + ") AS SourceType " +
        "FROM QSYS2. SYSPARTITIONSTAT " +
        "WHERE TRIM(SOURCE_TYPE) <> '' " + //TODO: Is this source_type validation right?
        "AND SYSTEM_TABLE_SCHEMA = '" + library + "' " +
        (sourcePf.isEmpty()? "" : "AND SYSTEM_TABLE_NAME = '" + sourcePf + "' ") +
        (members.isEmpty()? "" : "AND SYSTEM_TABLE_MEMBER IN (" + members.stream().map(m -> "'" + m + "'").collect(Collectors.joining(", ")) + ") ");
  }

  public void validateSourcePFs(String sourcePf, String library) throws SQLException{
    if (sourcePf.equals("")) throw new IllegalArgumentException("Source PF is empty");

    if ("QTEMP".equals(library.toUpperCase())) return; // Don't check for sourcPf in QTEMP

    // Validate if Source PF exists
    try (Statement validateStmt = connection.createStatement();
        ResultSet validateRs = validateStmt.executeQuery(
            "SELECT 1 AS Exist FROM QSYS2. SYSPARTITIONSTAT " +
                "WHERE SYSTEM_TABLE_SCHEMA = '" + library + "' " +
                "AND SYSTEM_TABLE_NAME = '" + sourcePf + "' " +
                "AND TRIM(SOURCE_TYPE) <> '' LIMIT 1")) {
      if (!validateRs.next()) {
        if (verbose) {
          System.err.println(" *Source PF " + sourcePf + " does not exist in library " + library);
          showSourcePFs(library); //Show available source PF in library
        }
        throw new IllegalArgumentException("Source PF " + sourcePf + " does not exist in library " + library);
      }

    }
  }

  public void showSourcePFs(String library) throws SQLException {
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

  public void validateMembers(String library, String sourcePf, List<String> members) throws SQLException {
    if (members.isEmpty()) throw new IllegalArgumentException("Member's list is empty");

    String inClause = members.stream().map(m -> "'" + m + "'").collect(Collectors.joining(", "));
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT CAST(SYSTEM_TABLE_MEMBER AS VARCHAR(10) CCSID " + SourceMigrator.INVARIANT_CCSID + ") AS Member " +
             "FROM QSYS2.SYSPARTITIONSTAT " +
             "WHERE SYSTEM_TABLE_SCHEMA = '" + library + "' " +
             "AND SYSTEM_TABLE_NAME = '" + sourcePf + "' " +
             "AND SYSTEM_TABLE_MEMBER IN (" + inClause + ") " +
             "AND TRIM(SOURCE_TYPE) <> '' ")) { //TODO: Add source type as param?
        Set<String> found = new HashSet<>();
        while (rs.next()) {
            found.add(rs.getString("Member").trim().toUpperCase());
        }
        List<String> missing = members.stream().map(m -> m.trim().toUpperCase()).filter(m -> !found.contains(m)).collect(Collectors.toList());
        if (!missing.isEmpty()) {
            if (verbose) System.err.println("Missing members in PF " + sourcePf + ": " + missing);
            
            throw new IllegalArgumentException("Some members do not exist in PF " + sourcePf + " in library " + library + ": " + missing);
        }
    }
  }
  
  // TODO: Add params validation to this class
  public void validateLibrary(String library) throws SQLException {
    if ("QTEMP".equals(library.toUpperCase())) return; // QTEMP is valid

    try (Statement validateStmt = connection.createStatement();
        ResultSet validateRs = validateStmt.executeQuery(
            "SELECT 1 AS Exists " +
                "FROM QSYS2. SYSPARTITIONSTAT " +
                "WHERE SYSTEM_TABLE_SCHEMA = '" + library + "' LIMIT 1")) {
      if (!validateRs.next()) {
        //TODO: Change err ouptut to something usefule
        if (verbose) {
          System.err.println(" *Library " + library + " does not exist in your system.");
          // Show similar libs
          try (Statement relatedStmt = connection.createStatement();
              ResultSet relatedRs = relatedStmt.executeQuery(
                  "SELECT SYSTEM_TABLE_SCHEMA AS library " +
                      "FROM QSYS2. SYSPARTITIONSTAT " +
                      "WHERE SYSTEM_TABLE_SCHEMA LIKE '%" + library + "%' " +
                      "GROUP BY SYSTEM_TABLE_SCHEMA LIMIT 10")) {
            if (relatedRs.next()) {
              System.err.println("Did you mean: ");
              do {
                System.err.println(relatedRs.getString("library").trim());
              } while (relatedRs.next());
            }
          }
        }
        throw new IllegalArgumentException("Library " + library + " does not exist in your system.");
      }
    }
    return;
  }

}
