"""Redis is an in-memory key-value store.
It can be used in cases like:
 * cache server
 * job queue
 * message queue

This module demonstrates how to use Redis to implement job queue.

Setup Redis:
 1. refer to https://redis.io/download
 2. [Optional] modify redis-4.0.0/redis
    a. setup password: requirepass your_password
    b. setup AOF/ RDB persistence
    c. setup master/ slave
    d. setup Redis Sentinel/ Redis Cluster. Note that data may be inconsistent upon network partition.

Note:
 * All classes below support stand alone Redis instance. Redis Cluster can be supported with minor code change.
 * Master/ slave + auto failover may cause data loss upon network partition.
"""

import redis
from abc import ABCMeta, abstractmethod

# update your redis configuration here
REDIS_CONF = {"host": "localhost",
              "port": 6379,
              "password": None,
              "db": 0}


class AbstractJobQueue(metaclass=ABCMeta):
    def __init__(self, queue_name):
        self.conn = redis.Redis(host=REDIS_CONF["host"], port=REDIS_CONF["port"], password=REDIS_CONF["password"],
                                db=REDIS_CONF["db"])
        self._queue_name = queue_name

    @abstractmethod
    def add(self, *args, **kwargs):
        """Add a job to queue."""
        return NotImplemented

    @abstractmethod
    def pop(self, *args, **kwargs):
        """Pop next job in queue. First in first out."""
        return NotImplemented

    @abstractmethod
    def get_all_job(self, *args, **kwargs):
        """Return a list of all jobs in current job queue."""
        return NotImplemented

    @abstractmethod
    def get_size(self, *args, **kwargs):
        """Return the size of current job queue."""
        return NotImplemented

    def delete(self):
        """Delete job queue."""
        self.conn.delete(self._queue_name)


class JobQueue(AbstractJobQueue):
    """Distributed Job Queue.
    Implemented by list.
    Used Redis Transaction to avoid race condition."""
    def __init__(self, queue_name="job_queue"):
        super(JobQueue, self).__init__(queue_name)

    def add(self, job):
        self.conn.rpush(self._queue_name, job)

    def pop(self):
        return self.conn.lpop(self._queue_name)

    def get_all_job(self):
        return self.conn.lrange(self._queue_name, 0, -1)

    def get_size(self):
        return self.conn.llen(self._queue_name)


class PriorityJobQueue(AbstractJobQueue):
    """Distributed Priority Job Queue.
    Implemented by sorted set.
    Used Redis Transaction to avoid race condition."""
    def __init__(self, descend=True, queue_name="priority_job_queue"):
        super(PriorityJobQueue, self).__init__(queue_name)
        self.descend = descend

    def add(self, job, priority):
        self.conn.zadd(self._queue_name, job, - priority if self.descend else priority)

    def pop(self):
        """Pop next job in queue, which has the highest priority."""
        while True:
            try:
                with self.conn.pipeline() as pipe:
                    pipe.watch(self._queue_name)
                    job = pipe.zrange(self._queue_name, 0, 1)[0]
                    pipe.multi()
                    pipe.zrem(self._queue_name, job)
                    pipe.execute()
                    return job
            except redis.WatchError:
                continue

    def get_all_job(self):
        return self.conn.zrange(self._queue_name, 0, -1)

    def get_size(self):
        return self.conn.zcard(self._queue_name)

if __name__ == "__main__":
    queue = JobQueue()
    queue.add("job1")
    queue.add("job2")
    queue.add("job3")
    assert queue.get_size() == 3
    assert queue.pop() == b"job1"
    assert queue.get_all_job() == [b"job2", b"job3"]
    queue.delete()

    priority_queue = PriorityJobQueue()
    priority_queue.add("job1", 0.0)
    priority_queue.add("job2", 2.0)
    priority_queue.add("job3", 1.0)
    assert priority_queue.get_size() == 3
    assert priority_queue.pop() == b"job2"
    assert priority_queue.get_all_job() == [b"job3", b"job1"]
    priority_queue.delete()
