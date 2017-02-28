Kinetica UDF Java API 
=====================

There are two projects in this repository: the Kinetica UDF Java API and an
example project.

proc-api
--------

In the proc-api directory, run the following command in the api direcotry to
create the API JAR:

> mvn clean package

Note that due to native components this must be run on Linux with the same
architecture as the Kinetica servers on which the UDFs will be used and a
compatible glibc version.


In order to use the API JAR for the example, run the following command to
install the jar in the local repository:

> mvn install


The documentation can be found at http://www.kinetica.com/docs/6.0/index.html.
UDF-specifid specific documentation can be found at:

*   http://www.kinetica.com/docs/concepts/index.html#user-defined-functions



proc-example
------------

The proc-example copies one or more input tables to output tables. If the
output tables exist, they must have compatible columns to the input tables.


To build the jar, run the following command in the proc-example directory:

> mvn clean package


This will produce a jar, kinetica-proc-example-1.0-jar-with-dependencies.jar,
in the target directory. This jar can be uploaded to Kinetica via the gadmin
UDF tab by clicking "New" and using the following parameters:

*    Name: proc-example (or as desired)
*    Command: java
*    Argument 1: -jar
*    Argument 2: kinetica-proc-example-1.0-jar-with-dependencies.jar
*    Files: kinetica-proc-example-1.0-jar-with-dependencies.jar


Once uploaded, the proc can be executed via gadmin.
