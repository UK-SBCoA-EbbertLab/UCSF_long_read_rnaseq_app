# File: app/tabs/__init__.py
# This file initializes the tabs for the application.

from app.tabs.tab0 import layout as tab0_layout
from app.tabs.tab1 import layout as tab1_layout
from app.tabs.tab2 import layout as tab2_layout

# Define the tabs we want to include
tabs = {
    "tab0": tab0_layout,
    "tab1": tab1_layout,
    "tab2": tab2_layout,
}

# This file can be empty, or you can directly import tab modules if needed
# You could import tab1 here if you want to make it directly available when importing app.tabs
