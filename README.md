# Scala Spark 1.6.x Example Project #
This contains enough boilerplate code to get you started with Apache Spark 1.6.x and Scala.

If you need to use Spark 2.0.0 or later, this project will not work for you.
See this instead: https://github.com/mikestaszel/spark-scala-boilerplate

## Building ##
Run the following:

    sbt assembly

Your JAR file to use with `spark-submit` will be: `target/scala-2.10/hellospark-assembly-1.0.jar`.

## Using in IntelliJ ##
To use this project in IntelliJ, import the project from existing sources using "sbt". Ensure that the Scala plugin is
installed first. IntelliJ will download `sbt` for you.
