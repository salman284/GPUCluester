ARC CLUSTER  -  Master / Worker software
=========================================
Folder-ta 4-ta PC-ei copy korun (jemon C:\ARC-Cluster). Python 3.9+ lagbe.

DHAP 1 -- Prottek PC-te (Master + 3 Worker)
  install.bat e double-click   (numpy + pyopencl install hobe)
  check_gpu.bat e double-click (AMD Radeon RX 580 dekhale GPU mode kaj korbe;
                                na dekhale AMD driver/OpenCL runtime install korte hobe.
                                tokhon software CPU mode-e cholbe -- Engine column-e dekha jabe)

DHAP 2 -- MASTER PC
  open_firewall_master.bat -> Right click -> "Run as administrator" (ekbar)
  start_master.bat chalan -> browser khulbe (http://localhost:8000)
  Prothombar admin User ID + Password toiri korun. Porer bar sei ID/Password diye login.
  Console-e Master-er IP:port lekha thakbe (jemon 172.10.22.15:8000) -- eta Worker-ra lagbe.
  Master PC nijeo "MASTER" node hishebe list-e thakbe (Disable korle Master-er GPU kaj korbe na).

DHAP 3 -- prottek WORKER PC
  start_worker.bat chalan -> browser khulbe (http://localhost:8001)
  Master address (172.10.22.15:8000), Worker ID (WORKER-1 / WORKER-2 / WORKER-3 -- alada alada),
  Password likhe "Register / Login" chapun.
  Master-e ei PC-r IP, CPU, RAM, GPU chole jabe.

DHAP 4 -- MASTER dashboard
  Nodes table-e register howa Worker dekhben (IP, CPU, RAM, GPU shoho).
  Je PC gulo use korben tader "Approve" chapun. Approve na korle kaj pabe na.
  Job section-e task sonkhya (100) diye "Start job" chapun.
  Progress, prottek node koyta task korlo, ar shesh-e COMPLETE 100/100 dekhabe. Report CSV download kora jay.

Database: Master-e master.db, prottek Worker-e worker.db (alada).
Notes:
 - Task gulo prottek ekbar-i assign hoy; worker disconnect korle 120s por task onno node pay.
 - Result Master-e recompute kore validate hoy.
 - Eta LAN-er jonno. Internet-e expose korben na. Worker-er password worker.db-te plain thake.
 - Ekhon kaj "demo": GPU-te x*x+2x+1 hishab. ARC-AGI solver boshate common.py-r run_task() function change korte hobe.
 - GPU shotti use hocche kina: Worker page / Master Nodes table-er "Engine" column dekhun ("GPU: ..." ba "CPU (numpy)").
 - 4-ta 8GB GPU mile 32GB hoy na; eta task-parallel system.
