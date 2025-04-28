# File: wsgi.py
# The main entry point for Heroku deployment.

# Ensure proper dependency initialization
import dash
import dash_bootstrap_components as dbc
import os
import sys

# Add debug prints to help troubleshoot
print("Python version:", sys.version)
print("Current working directory:", os.getcwd())
print("Files in current directory:", os.listdir("."))
print("Files in app directory:", os.listdir("app") if os.path.exists("app") else "app directory not found")
print("Environment variables:", [f"{k}={v}" for k, v in os.environ.items() if "DATABASE" in k])

# Add after the other debug prints, before importing app
try:
    # Explicitly check database connection
    import psycopg2
    import os
    
    # Print database environment variables (with sensitive parts redacted)
    database_url = os.environ.get('DATABASE_URL', 'Not set')
    if database_url != 'Not set':
        # Redact password for security
        parts = database_url.split(':')
        if len(parts) >= 3:
            redacted_url = f"{parts[0]}:{parts[1]}:***@{database_url.split('@')[1]}"
            print(f"DATABASE_URL is set: {redacted_url}")
    else:
        print("DATABASE_URL is not set")
    
    # Try to connect to the database
    if database_url != 'Not set':
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        print("Database connection successful!")
        cursor.close()
        conn.close()
except Exception as e:
    print(f"Database connection error: {e}")

# Now import the app
from app import app, server  # This is the key: explicitly import both app and server

# Add more debugging prints
print("Dash version:", dash.__version__)
print("DBC version:", dbc.__version__)
print("App config:", {k: v for k, v in app.config.items() if k not in ["supress_callback_exceptions", "suppress_callback_exceptions"]})

# We must import these AFTER importing app to ensure callbacks are registered
from app.layout import layout
import app.callbacks  # This registers all the callbacks

# Set the app layout
app.layout = layout
print("Layout set, components:", list(app.layout.children.keys()) if hasattr(app.layout, "children") else "No children in layout")

# This is only needed for local development
if __name__ == "__main__":
    app.run_server(debug=True)