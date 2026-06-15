#!/bin/sh
# Maven Wrapper startup script
# ----------------------------------------------------------------------------

# Resolve links and find the project base directory
PRG="$0"
while [ -h "$PRG" ]; do
  ls=$(ls -ld "$PRG")
  link=$(expr "$ls" : '.*-> \(.*\)$')
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=$(dirname "$PRG")/"$link"
  fi
done
SAVED="$(pwd)"
cd "$(dirname "$PRG")/" >/dev/null || exit 1
APP_HOME="$(pwd -P)"
cd "$SAVED" >/dev/null || exit 1

# Set Java home
if [ -z "$JAVA_HOME" ]; then
  echo "JAVA_HOME is not set. Please set JAVA_HOME to your JDK 21 installation."
  echo "Example: export JAVA_HOME=/c/Program Files/Java/jdk-21.0.11"
  exit 1
fi

JAVACMD="$JAVA_HOME/bin/java"

# Maven wrapper configuration
MAVEN_OPTS="${MAVEN_OPTS} -Xmx1024m"
export MAVEN_OPTS

# Execute the wrapper
exec "$JAVACMD" \
  $MAVEN_OPTS \
  -classpath "$APP_HOME/.mvn/wrapper/maven-wrapper.jar" \
  org.apache.maven.wrapper.MavenWrapperMain \
  "$@"
