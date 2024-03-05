<h3 align="center" style="margin:0px">
	<img width="200" src="https://www.kinetica.com/wp-content/uploads/2018/08/kinetica_logo.svg" alt="Kinetica Logo"/>
</h3>
<h5 align="center" style="margin:0px">
	<a href="https://www.kinetica.com/">Website</a>
	|
	<a href="https://docs.kinetica.com/7.2/">Docs</a>
	|
	<a href="https://docs.kinetica.com/7.2/udf/java/writing/">UDF API Docs</a>
	|
	<a href="https://join.slack.com/t/kinetica-community/shared_invite/zt-1bt9x3mvr-uMKrXlSDXfy3oU~sKi84qg">Community Slack</a>   
</h5>


# Kinetica Java UDF API

-  [Overview](#overview)
-  [API](#api)
-  [Example](#example)
-  [UDF Reference Documentation](#udf-reference-documentation)
-  [Support](#support)
-  [Contact Us](#contact-us)


## Overview

This is the 7.2 version of the server-side Java UDF API for Kinetica.  UDFs
are server-side programs written in this API and then installed, configured, and
initiated through a separate client-side management API.  These two APIs are
independent of each other and do not need to be written in the same language;
e.g., a UDF can be written in the Java UDF API and managed in SQL (from
Workbench, KiSQL, or other database client).

The source code for this project can be found at
https://github.com/kineticadb/kinetica-udf-api-java

For changes to the client-side API, please refer to
[CHANGELOG.md](CHANGELOG.md).


## API

This repository contains the Kinetica Java UDF API in the `proc-api` directory.

In the `proc-api` directory, run the following command to create the API JAR:

    mvn clean package

Note that due to native components this must be run on Linux with the same
architecture as the Kinetica servers on which the UDFs will be used and a
compatible glibc version.


In order to use the API JAR for the example, run the following command to
install the jar in the local repository:

    mvn install


## Example

This repository also contains an example project in the `proc-example`
directory, which implements a UDF in the Java UDF API.

This example copies one or more input tables to an output table. If the
output table exists, it must have compatible columns to the input tables.

To build the JAR, run the following command in the `proc-example` directory:

    mvn clean package

This will produce a JAR, `kinetica-proc-example-7.2-jar-with-dependencies.jar`,
in the `target` directory. This JAR can be uploaded to Kinetica via the GAdmin
*UDF* tab by clicking *New* and using the following parameters:

* **Name**: `proc-example` (or as desired)
* **Command**: `java`
* **Arguments**

  * **1**: `-jar`
  * **2**: `kinetica-proc-example-7.2-jar-with-dependencies.jar`

* **Files**: `kinetica-proc-example-7.1-jar-with-dependencies.jar`

Once uploaded, the UDF can be executed via GAdmin.


## UDF Reference Documentation

For information about UDFs in Kinetica, please see the User-Defined Functions
sections of the Kinetica documentation:

* **UDF Concepts**:  https://docs.kinetica.com/7.2/udf_overview/
* **Java UDF API**:  https://docs.kinetica.com/7.2/udf/java/writing/
* **Java UDF Management API**:  https://docs.kinetica.com/7.2/udf/java/running/
* **Java UDF Tutorial**:  https://docs.kinetica.com/7.2/guides/udf_java_guide/
* **Java UDF Examples**:  https://docs.kinetica.com/7.2/udf/java/examples/


## Support

For bugs, please submit an
[issue on Github](https://github.com/kineticadb/kinetica-udf-api-java/issues).

For support, you can post on
[stackoverflow](https://stackoverflow.com/questions/tagged/kinetica) under the
``kinetica`` tag or
[Slack](https://join.slack.com/t/kinetica-community/shared_invite/zt-1bt9x3mvr-uMKrXlSDXfy3oU~sKi84qg).


## Contact Us

* Ask a question on Slack:
  [Slack](https://join.slack.com/t/kinetica-community/shared_invite/zt-1bt9x3mvr-uMKrXlSDXfy3oU~sKi84qg)
* Follow on GitHub:
  [Follow @kineticadb](https://github.com/kineticadb) 
* Email us:  <support@kinetica.com>
* Visit:  <https://www.kinetica.com/contact/>
