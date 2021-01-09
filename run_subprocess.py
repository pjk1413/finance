import sys
import multiprocessing as multi
from Utility.config import global_dict

def start_server(app):
    app.run(host='0.0.0.0', port='5000', debug=True, use_reloader=True)

def add_process(name, func, args=[]):
    process = multi.Process(func, args=args)
    process.start()
    global_dict[name] = process

def terminate_process(name):
    if global_dict[name].is_alive():
        global_dict[name].terminate()
        return True
    else:
        return False

def run_process(arg):
    if arg == 'run_schedule':
        import Init.schedule as schedule
        schedule = multi.Process(schedule.run_schedule())
        add_process('schedule', schedule)
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

