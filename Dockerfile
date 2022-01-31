FROM python:3.8

WORKDIR /usr/src/app

COPY ./app .

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

RUN apt-get update && apt-get install -y cron
COPY sync-subscriptions-cron /etc/cron.d/sync-subscriptions-cron
RUN crontab /etc/cron.d/sync-subscriptions-cron

CMD ["cron", "-f"]