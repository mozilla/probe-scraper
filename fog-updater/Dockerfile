FROM python:3.10

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src/* ./

ENTRYPOINT ["/usr/src/app/fog_update.py"]
