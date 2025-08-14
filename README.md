# SourceMigrator

Tool for migrating IBM i source physical files ([PFs](https://github.com/kraudy/ibmi_os?tab=readme-ov-file#object-system-not-file-system)) to IFS stream files.

## Requirementes

* `Java 8` Most [IBM I](https://github.com/kraudy/ibmi_os) Shops have outdated Java.

## Install

* Download `.jar` file
* Upload `.jar` file using [Code4i](https://codefori.github.io/docs/#/) for drag and drop.

## Run

Open PASE terminal with Code4i

* `export QIBM_PASE_CCSID=1208` Set terminal ccsid
* `java -jar SourceMigrator.jar` Run tool

## Compile

* `git clone git@github.com:kraudy/SourceMigrator.git` Clone repo.
* `mvn clean package` Create .jar

## Contribute

Create an issue with the proposal and then a pull request.