FROM python:3.8-slim
RUN mkdir /app
RUN pip install fastapi uvicorn
WORKDIR /app
EXPOSE 80

COPY . .

RUN pip install -r requirements.txt
ADD . /app


CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]