import os
from dotenv import load_dotenv
from cryptography.fernet import Fernet

load_dotenv()
fernet_key = bytes(os.getenv('CRYPTO_KEY'), "utf-8")
fernet_instance = Fernet(fernet_key)


def encrypt_id(id_string):
    """Encrypt any ID (session or thread) to use as a token"""
    return fernet_instance.encrypt(id_string.encode()).decode()


def decrypt_id(encrypted_id):
    """Decrypt the token to get the original ID"""
    return fernet_instance.decrypt(encrypted_id.encode()).decode()
