import multiprocessing
import time

class tDif:
    def __init__(self):
        self.start_time = time.time()

    def get_end_time(self):
        print(time.time() - self.start_time)

class multiprocessing:
    def __init__(self, threads = 5):
        self.threads = threads
        self.jobs = []


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