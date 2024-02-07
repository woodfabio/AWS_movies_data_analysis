
FROM python:3.8-slim

WORKDIR /app

COPY requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

COPY upload_to_s3.py /app/

CMD ["python", "upload_to_s3.py"]
