@echo off
echo  BUILD IN WINDOWS
@echo off
setlocal

set OUT=exe
if exist %OUT% rmdir /s /q %OUT%
mkdir %OUT%

echo Building viks-windows-amd64.exe ...
set GOOS=windows
set GOARCH=amd64
set CGO_ENABLED=0
go build -trimpath -ldflags="-s -w" -o %OUT%\viks.exe .\viks

echo Building viks-linux-amd64 ...
set GOOS=linux
set GOARCH=amd64
go build -trimpath -ldflags="-s -w" -o %OUT%\viks-linux-amd64 .\viks

set GOOS=
set GOARCH=
set CGO_ENABLED=

echo Copying files ...
copy README.md %OUT%\ 
copy CHANGELOG.md %OUT%\
copy viks\credentials.json %OUT%\
copy viks\config.json %OUT%\


echo.
echo Done. Files in %OUT%:
dir /b %OUT%

endlocal