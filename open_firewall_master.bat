@echo off
netsh advfirewall firewall add rule name="ARC Cluster Master" dir=in action=allow protocol=TCP localport=8000 profile=private
pause
