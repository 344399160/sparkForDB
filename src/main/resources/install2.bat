@echo off

rem install htrace dependencies


echo install udps-sdk
pause
call mvn install:install-file "-DgroupId=udps" "-DartifactId=udps-sdk" "-Dversion=0.3" "-Dpackaging=jar" "-Dfile=D:\jra\udps-sdk-0.3-cdh5.4.0.jar"
