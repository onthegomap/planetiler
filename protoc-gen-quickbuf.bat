@echo off
SET mypath=%~dp0
java -jar "%mypath:~0,-1%\protoc-gen-quickbuf-1.0-rc1.jar"
