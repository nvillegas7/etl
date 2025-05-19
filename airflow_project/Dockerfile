# Dockerfile
FROM apache/airflow:2.8.3

USER root

# 1) install Chrome/Chromium + driver + needed libs for selenium
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      chromium \
      chromium-driver \
      # chrome deps:
      fonts-liberation libappindicator3-1 libatk-bridge2.0-0 \
      libatk1.0-0 libcairo2 libdbus-glib-1-2 libgtk-3-0 libnspr4 \
      libnss3 libx11-xcb1 libxcomposite1 libxcursor1 libxdamage1 \
      libxrandr2 xdg-utils \
 && rm -rf /var/lib/apt/lists/*

# (Don’t purge chromium-driver — webdriver_manager can cleanly 
#  download a matching driver at runtime.)

USER airflow

ENV PYTHONPATH=/opt/airflow

# 2) copy & install Python requirements
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
