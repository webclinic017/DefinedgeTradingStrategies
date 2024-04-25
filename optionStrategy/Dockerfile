# Start from python slim-buster docker image
FROM python:3.11.8-slim
# Update base packages
RUN apt-get update -y
RUN apt-get upgrade -y
# Change TimeZone
ENV TZ=Asia/Calcutta
ENV slack_client = " "
ENV quantity = 50
# ENV access_token = "xyz"
# ENV user_name = "xyz"
# ENV target_profit = 0
# ENV lock_trigger = 2000
# ENV lock_profit = 1000
# ENV CONNECTION_STRING = "mongodb+srv://adminuser:05NZN7kKp5D4TZnU@bots.vnitakj.mongodb.net/?retryWrites=true&w=majority"
# ENV trade_end_time = "15:14:00"
# ENV instrument_name = "BANKNIFTY"
# ENV slack_url = "https://hooks.slack.com/services/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"
# ENV slack_channel = "straddlebot"
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN echo $(date)
# Copy files to working directory
COPY . /app/src/
COPY ./lib /app/src/
WORKDIR /app
# Install python packages using requirements.txt
RUN pip install -r src/requirements.txt

# Run the script
CMD python src/NiftyPositional.py