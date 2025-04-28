# File: app.py
# The main entry point that sets the layout, registers callbacks, and runs the server.

# Use absolute imports to avoid naming conflicts
import app
from app.layout import layout
import app.callbacks  # This registers the callbacks

# Set the app layout to our defined layout
app.app.layout = layout

# This is only needed for local development
if __name__ == "__main__":
    app.app.run_server(debug=True)