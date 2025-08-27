package com.github.kraudy.migrator;

import com.ibm.as400.access.User;

import com.ibm.as400.access.User;

import java.sql.Connection;
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
}
