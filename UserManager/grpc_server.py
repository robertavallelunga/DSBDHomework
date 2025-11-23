from database import get_db_connection
import os
from concurrent import futures
import grpc
import user_pb2
import user_pb2_grpc

class UserService(user_pb2_grpc.UserServiceServicer):
    def CheckUserExists(self, request, context):
        """Verifica esistenza utente per il DataCollector"""
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM users WHERE email = %s", (request.email,))
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return user_pb2.UserResponse(exists=(count > 0))

def serve_grpc():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    user_pb2_grpc.add_UserServiceServicer_to_server(UserService(), server)
    port = os.getenv('GRPC_PORT', '50051')
    server.add_insecure_port(f'[::]:{port}')
    print(f"gRPC Server avviato nella porta {port}")
    server.start()
    server.wait_for_termination()