
# Redis Job Queue

Distributed (Priority) Job Queue implemented with Redis

## Distributed Job Queue
Distributed Job Queue is used to load-balance works among workers by dynamically assigning jobs.

This repo includes a FIFO (first in first out) job queue and a priority job queue.

## Scalability
With Redis as backend, distributed workers can request job from the queue.
Redis Cluster will be supported soon, which further increases scalability.

## Dependencies
This project depends on the following packages:

    * Python==3.6
    * redis==3.2.0
