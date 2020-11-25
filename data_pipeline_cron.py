from crontab import CronTab
#Set The User To Run The Cron Job
cron = CronTab(user='morris')
#Set The Command To Be Run
job = cron.new(command='python data_pipeline.py dev')
#Set The Time For The Cron Job
job.minute.every(2)
#Write The New Job To CronTab
cron.write()