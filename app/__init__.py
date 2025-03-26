import dash
import dash_bootstrap_components as dbc
import atexit
from app.utils.db_utils import cleanup

# Create Dash application
app = dash.Dash(
    __name__,
    suppress_callback_exceptions=True,
    external_stylesheets=[dbc.themes.SOLAR]
)
server = app.server

# Register cleanup function to be called when the app exits
atexit.register(cleanup)
