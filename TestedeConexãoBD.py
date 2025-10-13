import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()

try:
    conn = mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_DATABASE'),
        port=3306
    )
    print("✅ Conexão com MySQL estabelecida com sucesso!")
    conn.close()
except mysql.connector.Error as err:
    print(f"❌ Erro na conexão: {err}")
