FROM python:3.12-slim
WORKDIR /app
COPY SLABreachDetector/ .
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "SLABreachDetector.py"]