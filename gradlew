#!/bin/sh
APP_HOME=$(cd "${0%/*}" && pwd)
DEFAULT_JVM_OPTS="-Xmx64m -Xms64m"
JAVA_EXE=java
if [ -n "$JAVA_HOME" ]; then
  JAVA_EXE="$JAVA_HOME/bin/java"
fi
"$JAVA_EXE" $DEFAULT_JVM_OPTS -classpath "$APP_HOME/gradle/wrapper/gradle-wrapper.jar" org.gradle.wrapper.GradleWrapperMain "$@"
