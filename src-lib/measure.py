import functools
import datetime
import logging
'''
import os
import sys
path = os.path.dirname(os.path.abspath(__file__))
print(path)
app_path = os.path.split(path)[0]
print(app_path)
sys.path.append(app_path)
'''
logger = logging.getLogger('logfile')

def measure_time_old(func):
    @functools.wraps(func)
    def measure_time_elapsed(*args, **kwargs):
        api_start_time = datetime.datetime.now().timestamp()
        
        result = func(*args, **kwargs)
        api_end_time = datetime.datetime.now().timestamp()
        logger.debug('************************* ' + str((api_end_time -api_start_time) * 1000))
        return result
    return measure_time_elapsed


def measure_time_with_path(*args, **kwargs):
    # print('printing arguments - ' + args[0])
    path1 = args[0]
    def measure_time(func):
        @functools.wraps(func)
        def measure_time_elapsed(*args, **kwargs):
            api_start_time = datetime.datetime.now().timestamp()
            # print(api_start_time)
            result = func(*args, **kwargs)
            api_end_time = datetime.datetime.now().timestamp()
            logger.debug(path1 + ' ' + str((api_end_time -api_start_time) * 1000))
            return result
        return measure_time_elapsed
    return measure_time