import os
import uuid
from dotenv import load_dotenv
from cryptography.fernet import Fernet


def generate_encrypted_token():
    load_dotenv()
    uuid_token = uuid.uuid4()
    fernet_key = bytes(os.getenv('CRYPTO_KEY'), "utf-8")
    fernet_instance = Fernet(fernet_key)
    encrypted_token = fernet_instance.encrypt(bytes(str(uuid_token), 'utf-8'))
    return encrypted_token


def decrypt_token(encrypted_token: bytes):
    load_dotenv()
    fernet_key = os.getenv('CRYPTO_KEY')
    fernet_instance = Fernet(fernet_key)
    decrypted_token = fernet_instance.decrypt(encrypted_token)
    return decrypted_token


def match_encrypted_token(encrypted_token: bytes):
    """
    :param encrypted_token:
    :return: bool
    """
    decrypted_token = decrypt_token(encrypted_token)
    token_match = "check if decrypted token exists in db. if true, then return True. else False"
    return token_match

