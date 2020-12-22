import multiprocessing
import time

class multiprocessing:
    def __init__(self, threads = multiprocessing.cpu_count() - 2):
        self.o = None


    def start(self):
        for j in self.jobs:
            j.start()

        for j in self.jobs:
            j.join()


    def add_job(self, function, args: ()):
        self.jobs.append(multiprocessing.Process(target=function, args=args))
        if len(self.jobs) >= self.threads:
            self.start()


class multiprocessing:
    def __init__(self, processes = 5):
        self.processes = processes
        self.jobs = []