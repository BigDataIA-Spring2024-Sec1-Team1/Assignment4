# FROM apache/airflow:2.5.1
# USER root
# # Base docker image of python 3.x

# #RUN apt-get -y update; apt-get -y upgrade; apt-get -y install curl libsndfile1 flac ffmpeg
# # Update the apt-get package and install curl for healthcheck

# RUN pip install --upgrade pip
# # Upgrade pip package


# COPY requirements.txt /
# # Copy main.py and requirements.txt from local into app dir inside the container

# USER airflow
# RUN pip install -r requirements.txt
# # Referring to copied file inside the app dir install the user dependency

# EXPOSE 8095
# # Expose a port inside the container on which services run

# # CMD ["uvicorn", "main:app", "--proxy-headers", "--host", "0.0.0.0", "--port", "8095", "--timeout-keep-alive", "1"]
# # gunicorn command to run the service with 4 worker nodes binding localhost/0.0.0.0 on port 8095 referring app in


FROM apache/airflow:2.5.1
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /
USER airflow
RUN pip install --user -r /requirements.txt

