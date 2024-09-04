print("Starting run.py")
try:
    from src.backend.api import app
    print("Imported app successfully")
except Exception as e:
    print(f"Error importing app: {e}")
    import sys
    sys.exit(1)

if __name__ == "__main__":
    print("About to start uvicorn server")
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)