FROM python:3.8
WORKDIR /app
COPY ./API/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ./API .

CMD ["python", "-u", "app.py"]