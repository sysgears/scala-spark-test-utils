name := "spark-cucumber"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "io.cucumber"      %  "cucumber-core"  % "6.7.0",
  "io.cucumber"      %  "cucumber-java"  % "6.7.0",
  "org.apache.spark" %% "spark-sql"      % "2.2.1",
  "org.mockito"      %  "mockito-core"   % "3.5.13",
  "org.mockito"      %  "mockito-inline" % "3.5.13",

  "org.scalatest"    %% "scalatest"      % "3.2.0"  % "test",
  "com.novocode"     % "junit-interface" % "0.11"   % "test",
  "io.cucumber"      %  "cucumber-junit" % "6.7.0"  % "test",
  "io.cucumber"      %  "cucumber-guice" % "6.7.0"  % "test"
)