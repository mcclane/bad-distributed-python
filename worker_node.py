import asyncio


class Job:
    def __init__(self, cmd, max_reruns=None):
        self.cmd = cmd
        if max_reruns == None:
            self.max_reruns = 3
        else:
            self.max_reruns = max_reruns
        self.runs = 0
        self.result = None

    def __str__(self):
        return self.cmd

class RemoteJobResult:
    def __init__(self, communication_error=None, stdout=None, stderr=None, return_code=None, dead_worker_node=None, successful=None, rerun=None):
        self.communication_error = communication_error
        self.stdout = stdout
        self.stderr = stderr
        self.return_code = return_code
        self.dead_worker_node = dead_worker_node
        self.successful = successful
        self.rerun = rerun

    def __str__(self):
        return f"RemoteJobResult: {self.return_code} {self.stdout} {self.stderr}"

class WorkerNode:
    def __init__(self, hostname, max_jobs):
        self.hostname = hostname
        self.max_jobs = max_jobs
        self.running_jobs = set()
        self.alive = False

    async def check_alive(self):
        job = await self.run_remote_job(Job("date"))
        res = job.result
        if res.stdout and not res.stderr and not res.return_code:
            self.alive = True
        else:
            self.alive = False

    async def run_remote_job(self, job):
        print(f"Running {job} on {self.hostname}")
        self.running_jobs.add(job)
        cmd = f"ssh {self.hostname} {job.cmd}"
        proc = await asyncio.create_subprocess_shell(
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await proc.communicate()
        self.running_jobs.remove(job)
        job.result = RemoteJobResult(stdout=stdout, stderr=stderr, return_code=proc.returncode)
        print(job.result)
        return job
    
    def __str__(self):
        return f"Worker: {self.hostname}, number of running jobs: {len(self.running_jobs)}"

