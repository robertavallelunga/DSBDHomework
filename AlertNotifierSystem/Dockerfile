FROM python:3.12-slim
WORKDIR /app
COPY AlertNotifierSystem/ .
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "notifier_system.py"]