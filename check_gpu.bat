@echo off
cd /d "%~dp0"
python -c "import pyopencl as cl; [print(p.name, [d.name for d in p.get_devices()]) for p in cl.get_platforms()]"
pause
