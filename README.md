# resource-scheduler

This repository implements a `resource` scheduler for scheduling Pod which request `ResourceClass` resources.

This is a part of POC implementation of Resource Class with `CRD`, `Custom Scheduler` and `Device Plugin`.

## Build & Run

```sh
# build
$ make resource-scheduler

# run
# assumes you have a working kubeconfig, not required if operating in-cluster
$ ./_output/resource-scheduler --kubeconfig ${configfile}

# create a pod with nodeSelector and schedulerName
$ kubectl create -f examples/pod.yaml

```
