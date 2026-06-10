$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptDir

mvn spring-boot:run "-Dspring-boot.run.addResources=true"
