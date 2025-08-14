# SourceMigrator

Tool for migrating IBM i source physical files ([PFs](https://github.com/kraudy/ibmi_os?tab=readme-ov-file#object-system-not-file-system)) to IFS stream files.

## Requirementes

* `Java 8` Most [IBM I](https://github.com/kraudy/ibmi_os) Shops have outdated Java.

## Install

* `git clone git@github.com:kraudy/SourceMigrator.git` Clone repo.
* `Ctrl + Shift + e` using [Code4i](https://codefori.github.io/docs/#/). Deploy `.jar`

## Run

Open PASE terminal 

* `cd $HOME/builds/SourceMigrator/SourceMigrator/target` Move to deployed location
* `export QIBM_PASE_CCSID=1208` Set terminal ccsid
* `java -jar SourceMigrator-1.0-SNAPSHOT.jar` Run tool

## Compile

* `mvn clean package` Create .jar

## Contribute

Create an issue with the proposal and then a pull request.