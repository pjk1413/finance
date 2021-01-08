import sys
import multiprocessing as multi

class processor:
    def __init__(self):
        self.dict = {}

    def add_process(self, name, process):
        self.dict[name] = process

    def terminate_process(self, name):
        if self.dict[name].is_alive():
            self.dict[name].terminate()
            return True
        else:
            return False

    def run_process(self, arg):
        if arg == 'run_schedule':
            import Init.schedule as schedule
            schedule = multi.Process(schedule.run_schedule())
            self.add_process('schedule', schedule)
            print("Schedule is running...")

        if arg == 'retrieve_technical_data':
            import Data.Technical_Data.Retrieve_Data.retrieve_technical as rt

            try:
                rt.retrieve_technical_bulk().run_data_load(range='historical')
            except Exception:
                import sys
                print(sys.exc_info()[0])
                import traceback
                print(traceback.format_exc())
            finally:
                print("Press Enter to continue ...")
                input()

        if arg == 'retrieve_sentiment_data':
            import Data.Sentiment_Data.Retrieve_Data.retrieve_sentiment as rs
            try:
                rs.retrieve_sentiment_data().run_data_load(range='historical')
            except Exception:
                import sys
                print(sys.exc_info()[0])
                import traceback
                print(traceback.format_exc())
            finally:
                print("Press Enter to continue ...")
                input()

