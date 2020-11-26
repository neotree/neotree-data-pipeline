from crontab import CronTab
from common_files.config import config
import logging
from common_files.format_error import formatError
import sys
params = config()
cron_user = params['cron_user']
mode = params['env']
interval = int(params['cron_interval'])
try:
# Set The User To Run The Cron Job
    cron = CronTab(user=cron_user)
# Set The Command To Be Run
    job = cron.new(command='python data_pipeline.py {0}'.format(mode))
# Set The Time For The Cron Job
# Use job.minute for quick testing
    job.every(interval).minutes()
    # job.every(interval).hours
# Write The New Job To CronTab
    cron.write()
    logging.info("Cron Job is ready to start executing: ")

except Exception as e:
    logging.error("!!Cron Job Failed To Start Due To Errors: ")
    logging.error(formatError(e))
    sys.exit(1)
