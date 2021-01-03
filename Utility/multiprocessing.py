import multiprocessing
import datetime
import subprocess
import time

# def run(function, args=()):
#     func = multiprocessing.Process(target=function, args=args)
#     func.start()



class multi_processing:
    def __init__(self):
        self.list_of_processes = []
        self.max_process = multiprocessing.cpu_count() -1

    def add_process(self, function, shell=False):
        new_process = multiprocessing.Process(target=function, name=f'{datetime.datetime.now().strftime("%d_%M_%S")}')
        new_process.daemon = True
        if len(self.list_of_processes) > self.max_process:
            self.join_processes()
            return False
        new_process.start()
        self.list_of_processes.append(new_process)
        return True

    def join_processes(self):
        for process in self.list_of_processes:
            process.join()
