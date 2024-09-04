print("Initializing backend package")

try:
    print("Attempting to import from api.py")
    from .api import app
    print("Successfully imported app from api.py")
except Exception as e:
    print(f"Error importing from api.py: {e}")

__all__ = ['app']