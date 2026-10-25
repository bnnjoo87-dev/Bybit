FROM python:3.10-slim
WORKDIR /usr/src/app
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential python3-dev curl && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir --upgrade pip setuptools wheel
COPY requirements.txt .
RUN pip install --no-cache-dir --only-binary=all -r requirements.txt
COPY . .
RUN mkdir -p /data
ENV DATA_ROOT=/data PORT=8080
CMD ["python", "main.py"]
