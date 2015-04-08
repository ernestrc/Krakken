name := "toktok_core"

libraryDependencies ++= List(
  "com.typesafe.akka"               %% "akka-actor"             % "2.3.7",
  "com.typesafe.akka"               %% "akka-slf4j"             % "2.3.7",
  "io.spray"                        %% "spray-can"              % "1.3.2",
  "io.spray"                        %% "spray-routing"          % "1.3.2",
  "io.spray"                        %% "spray-caching"          % "1.3.2",
  "io.spray"                        %% "spray-util"             % "1.3.2",
  "io.spray"                        %% "spray-json"             % "1.3.0",
  "org.mongodb"                     %% "casbah"                 % "2.8.0",
  "com.novus"                       %% "salat"                  % "1.9.9",
  "org.scala-lang"                  % "scala-compiler"          % "2.11.6",
  "org.scala-lang.modules"          %% "scala-async"            % "0.9.2",
  "joda-time"                       % "joda-time"               % "2.5",
  "org.joda"                        % "joda-convert"            % "1.7",
  "com.typesafe"                    % "config"                  % "1.2.1",
  "ch.qos.logback"                  % "logback-classic"         % "1.1.2",
  "com.typesafe.akka"               %% "akka-testkit"           % "2.3.7"               % "test",
  "io.spray"                        %% "spray-testkit"          % "1.3.1"               % "test",
  "com.h2database"                  % "h2"                      % "1.3.173"             % "test",
  "org.hamcrest"                    % "hamcrest-all"            % "1.3"                 % "test",
  "org.scalacheck"                  %% "scalacheck"             % "1.11.3"              % "test",
  "junit"                           % "junit"                   % "4.7"                 % "test",
  "org.specs2"                      %% "specs2"                 % "2.4"                 % "test",
  "org.mockito"                     % "mockito-all"             % "1.9.0"               % "test",
  "com.novocode"                    % "junit-interface"         % "0.7"                 % "test->default"
)

parallelExecution in Test := false

testOptions in Test += Tests.Argument(TestFrameworks.Specs2, "junitxml", "console")
