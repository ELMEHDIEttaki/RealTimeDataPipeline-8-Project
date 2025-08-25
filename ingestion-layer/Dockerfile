FROM python:3.12.3-slim


WORKDIR /ingestion-layer

# Install system packages, including netcat
RUN apt-get update && apt-get install -y netcat-openbsd && rm -rf /var/lib/apt/lists/*
# Create a virtual environement
RUN python -m venv env


#COPY requirements.txt /ingestion-layer/requirements.txt
COPY . /ingestion-layer/

RUN . env/bin/activate && pip install --no-cache-dir -r /ingestion-layer/requirements.txt



RUN chmod +x /ingestion-layer/wait-for-kafka.sh

#ENTRYPOINT [ "/producer0/wait-for-kafka.sh", "kafka", "29092"]
ENTRYPOINT ["/ingestion-layer/wait-for-kafka.sh", "kafka", "29092", "150"]


CMD ["./env/bin/python", "main.py"]
#CMD [ "source env/bin/activate && python producer0" ]
