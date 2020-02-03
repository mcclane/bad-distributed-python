import asyncio
from socket import gethostname
from worker_node import *


class Leader:
    def __init__(self, loop, worker_nodes, job_commands, did_job_work_function):
        self.loop = loop
        self.did_job_work_function = did_job_work_function 
        self.worker_nodes = worker_nodes

        self.job_queue = asyncio.Queue()
        for cmd in job_commands:
            self.job_queue.put_nowait(Job(cmd))
    
    async def check_if_worker_nodes_are_alive(self):
        await asyncio.gather(*[worker.check_alive() for worker in self.worker_nodes])

    def anyone_working(self):
        for worker in self.worker_nodes:
            if len(worker.running_jobs) > 0:
                return True
        return False

    def get_available_worker(self):
        for worker in self.worker_nodes:
            if worker.alive and len(worker.running_jobs) < worker.max_jobs:
                return worker
        return None
    
    async def run_job(self, worker, job):
        response = await worker.run_remote_job(job)
        job = self.did_job_work_function(response)
        if job.result.dead_worker_node:
            worker.alive = False
        if job.result.rerun:
            self.job_queue.put_nowait(job)
        if job.result.successful:
            print(f"{job} on {worker} successful")
        else:
            print(f"{job} on {worker} failed")

    async def run_jobs(self):
        while True:
            worker = self.get_available_worker()
            while worker == None:
                await asyncio.sleep(0.25)
                worker = self.get_available_worker()
            job = await self.job_queue.get()
            if job.runs > job.max_reruns:
                print(f"Retry limit for job {job} exceeded")
            else:
                asyncio.create_task(self.run_job(worker, job))
            self.job_queue.task_done()

    def exit():
        self.loop.stop()

    async def main(self):
        await self.check_if_worker_nodes_are_alive()
        await self.run_jobs()


def did_job_work(job):
    print("hello?")
    if not job.result.stderr and not job.result.return_code:
        job.result.successful = True
        job.result.rerun = False
    return job

def main():
    worker_nodes = [
            WorkerNode("mhowland@unix1.csc.calpoly.edu", 2),
            WorkerNode("mhowland@unix3.csc.calpoly.edu", 2)
        ]
    job_cmds = ['ls' for i in range(2)]

    loop = asyncio.get_event_loop()
    leader = Leader(loop, worker_nodes, job_cmds, did_job_work) 
    loop.create_task(leader.main())
    loop.run_forever()

if __name__ == '__main__':
    main()
