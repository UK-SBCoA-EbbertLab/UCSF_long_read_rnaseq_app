# Simple entry point for local development
# Run this with: python run.py

from app import app

if __name__ == "__main__":
    app.run_server(debug=True) 