import os
import grpc
from flask import Flask, request, jsonify
from database import init_db, get_db_connection
import user_pb2
import user_pb2_grpc

app = Flask(__name__)

GRPC_HOST=os.getenv("TARGET_GRPC_HOST", "userManager")
GRPC_PORT=os.getenv("TARGET_GRPC_PORT", 50051)

def check_user_exists_grpc(email):
    """Chiama User Manager via gRPC per verificare l'utente"""
    channel = grpc.insecure_channel(f'{GRPC_HOST}:{GRPC_PORT}')
    stub = user_pb2_grpc.UserServiceStub(channel)
    try:
        response = stub.UserExists(user_pb2.UserRequest(email=email))
        return response.exists
    except grpc.RpcError as e:
        print(f"gRPC Error: {e}")
        return False

@app.route('/add', methods=['POST'])
def add_interest():
    """Aggiunge interesse per un aeroporto se l'utente esiste"""
    data = request.json
    email = data.get('email')
    airport = data.get('airport')

    # 1. Verifica gRPC
    if not check_user_exists_grpc(email):
        return jsonify({"error": "User non trovato nel database"}), 404

    # 2. Salva interesse
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO interests (email_user, cod_aeroporto) VALUES (%s, %s)", (email, airport))
    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({"message": "Aeroporto aggiunto"}), 201

if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5000)
