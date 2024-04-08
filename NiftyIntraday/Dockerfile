# Start from python slim-buster docker image
FROM python:3.11.8-slim
# Update base packages
RUN apt-get update -y
RUN apt-get upgrade -y
# Change TimeZone
ENV TZ=Asia/Calcutta
ENV slack_channel="straddlebot"
ENV slack_token=" "
ENV CONNECTION_STRING="mongodb+srv://adminuser:05NZN7kKp5D4TZnU@bots.vnitakj.mongodb.net/?retryWrites=true&w=majority"
ENV api_token="xyz"
ENV api_secret="xyz"
ENV instrument_name="xyz"
ENV trading_symbol="xyz"
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN echo $(date)
# Copy files to working directory
COPY . /app/src/
COPY ./lib /app/src/
WORKDIR /app
# Install python packages using requirements.txt
RUN pip install -r src/requirements.txt
# Run the script
CMD python src/NiftyForwardTest.py