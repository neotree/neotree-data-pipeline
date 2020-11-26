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
    cron = CronTab(user=str(cron_user))
# Set The Command To Be Run
    job = cron.new(command='python data_pipeline.py {0}'.format(mode))
# Set The Time For The Cron Job
# Use job.minute for quick testing
    job.minute.every(interval)
# job.every(interval).hours
# Run The New Job To CronTab
    job.run()
    
except Exception as e:
    logging.error("!!Cron Job Failed To Start Due To Errors: ")
    logging.error(formatError(e))
    sys.exit(1)

