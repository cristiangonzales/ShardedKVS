# Order of which to run scripts

1. Initialize network: `Docker-network-init.sh`
2. Run multiple instances: `Docker-instance*.sh`
3. Kill the network (after all instances have been killed): `Docker-network-kill.sh`

* To emulate network partitions, run `Emulate-network-partition.sh`, and to run test requests, run `Test-requests.sh` (remember to change the PORT variable to which instance you are forwarding requests). Legacy files can be found in the `legacy/` subdirectory.
