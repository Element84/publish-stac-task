FROM python:3.9-alpine

WORKDIR /task

COPY requirements.txt requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY task.py .

CMD ["python3", "./task.py", "run"]
