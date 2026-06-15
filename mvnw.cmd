@echo off
setlocal

set "APP_HOME=%~dp0"

if not defined JAVA_HOME (
    echo JAVA_HOME is not set. Please set JAVA_HOME to JDK 21.
    exit /b 1
)

set "JAVACMD=%JAVA_HOME%\bin\java.exe"

if not exist "%APP_HOME%.mvn\wrapper\maven-wrapper.jar" (
    echo Maven wrapper JAR not found
    exit /b 1
)

"%JAVACMD%" -Dmaven.multiModuleProjectDirectory="%APP_HOME%" %MAVEN_OPTS% -classpath "%APP_HOME%.mvn\wrapper\maven-wrapper.jar" org.apache.maven.wrapper.MavenWrapperMain %*
