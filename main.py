import pandas as pd
import sqlite3
import schedule
import time
import logging
import jobs
import os


if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO,filename='app.log', filemode='w', format='%(asctime)s %(name)s - %(levelname)s - %(message)s')
    logging.info('Application started')

    try:
        os.mkdir('input')
        os.mkdir('input')
        os.mkdir('processed')
        print('Directories created.')
    except:
        print('All directories are already created')
        pass



    try:
        jobs.myvars()
    except:
        logging.info('Connection failed')
        pass

    try:
        schedule.every(3).seconds.do(jobs.job)
    except Exception as e:
        logging.warning("something raised an exception: " , exc_info=True)
        pass
    while True:
        schedule.run_pending()
        time.sleep(1)
