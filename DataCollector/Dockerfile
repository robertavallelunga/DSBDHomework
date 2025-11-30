FROM python:3.12-slim
WORKDIR /app
COPY DataCollector/ .
COPY protos/user.proto .
RUN pip install --no-cache-dir -r requirements.txt
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. user.proto
CMD ["python", "app.py"]