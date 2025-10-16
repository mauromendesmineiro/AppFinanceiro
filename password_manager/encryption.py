from cryptography.fernet import Fernet
import os

# Define o nome do arquivo da chave
KEY_FILE = "secret.key"

def load_or_generate_key():
    """Carrega a chave do arquivo ou a gera se não existir."""
    if os.path.exists(KEY_FILE):
        with open(KEY_FILE, "rb") as key_file:
            key = key_file.read()
    else:
        key = Fernet.generate_key()
        with open(KEY_FILE, "wb") as key_file:
            key_file.write(key)
    return key

# Carrega a chave ao iniciar o módulo
KEY = load_or_generate_key()

def encrypt_data(data, key):
    """Criptografa os dados usando a chave fornecida."""
    f = Fernet(key)
    return f.encrypt(data.encode())

def decrypt_data(encrypted_data, key):
    """Descriptografa os dados usando a chave fornecida."""
    f = Fernet(key)
    return f.decrypt(encrypted_data).decode()