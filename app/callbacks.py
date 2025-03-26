# File: app/callbacks.py
# Contains the callback to update the content based on the selected tab.

from dash.dependencies import Input, Output, State
from dash import callback_context, html, dcc
from app import app
from app.layout import content_layout
import pandas as pd
import polars as pl
from polars import col, lit
import dash_bootstrap_components as dbc
from app.utils.db_utils import get_gene_data_with_metadata, duck_conn, POLARS_AVAILABLE
from app.utils.polars_utils import order_transcripts_by_expression
import RNApysoforms as RNApy

# Callback to update the active tab when a nav link is clicked
@app.callback(
    Output("active-tab", "data"),
    [
        Input("nav-1", "n_clicks"),
        Input("nav-2", "n_clicks"),
        Input("nav-3", "n_clicks"),
        Input("nav-4", "n_clicks"),
        Input("nav-5", "n_clicks"),
        Input("nav-6", "n_clicks")
    ],
    [State("active-tab", "data")]
)
def update_active_tab(n1, n2, n3, n4, n5, n6, current_tab):
    # Get the ID of the clicked nav item
    ctx = callback_context
    if not ctx.triggered:
        return current_tab
    
    button_id = ctx.triggered[0]["prop_id"].split(".")[0]
    
    # Map nav ID to tab ID
    tab_mapping = {
        "nav-1": "tab-1",
        "nav-2": "tab-2",
        "nav-3": "tab-3",
        "nav-4": "tab-4",
        "nav-5": "tab-5",
        "nav-6": "tab-6"
    }
    
    return tab_mapping.get(button_id, current_tab)

# Callback to update nav link active states
@app.callback(
    [
        Output("nav-1", "active"),
        Output("nav-2", "active"),
        Output("nav-3", "active"),
        Output("nav-4", "active"),
        Output("nav-5", "active"),
        Output("nav-6", "active")
    ],
    [Input("active-tab", "data")]
)
def update_nav_active(active_tab):
    active_states = {
        "tab-1": [True, False, False, False, False, False],
        "tab-2": [False, True, False, False, False, False],
        "tab-3": [False, False, True, False, False, False],
        "tab-4": [False, False, False, True, False, False],
        "tab-5": [False, False, False, False, True, False],
        "tab-6": [False, False, False, False, False, True]
    }
    
    return active_states.get(active_tab, [False, False, False, False, False, False])

# Callback to update the content based on the active tab
@app.callback(Output("content", "children"), [Input("tabs", "active_tab")])
def render_content(tab):
    # Return the layout corresponding to the selected tab, or a default message if not found.
    return content_layout.get(tab, "Tab not found")

# Add this new callback
@app.callback(
    Output('matrix-content', 'children'),
    [Input('matrix-table-dropdown', 'value'),
     Input('search-input', 'value')]
)
def update_matrix_content(selected_table, selected_gene):
    if not selected_table:
        return html.P("Please select a matrix table", 
                     style={"color": "#666666"})
    
    if not selected_gene:
        return html.P("Please select a gene to display data", 
                     style={"color": "#666666"})
    
    try:
        # Verify the gene exists using our compatibility wrapper
        gene_info = duck_conn.execute("""
            SELECT gene_id, gene_name 
            FROM transcript_annotation 
            WHERE gene_id = ?
            LIMIT 1
        """, [selected_gene]).fetchone()
        
        if not gene_info:
            return html.P(f"Gene ID '{selected_gene}' not found in the annotation table", 
                        style={"color": "#666666"})
        
        actual_gene_id, gene_name = gene_info
        
        # First get a row count - this helps assess the data size 
        # and provides immediate feedback to the user
        row_count = duck_conn.execute("""
            SELECT COUNT(*) 
            FROM {}
            WHERE gene_id = ?
        """.format(selected_table), [actual_gene_id]).fetchone()[0]
        
        if row_count == 0:
            return html.P(f"No data found for gene {gene_name} ({actual_gene_id})", 
                        style={"color": "#666666"})
        
        # Get the matrix data - limit to first 50 rows for better performance
        max_rows = 50  # Limit the number of rows for display
        data_loaded = False
        using_polars = False
        has_metadata = False
        err_msg = None
        
        # Always try with Polars first - it's faster and more memory efficient
        try:
            df = get_gene_data_with_metadata(actual_gene_id, selected_table, with_polars=True, limit=max_rows)
            using_polars = True
            data_loaded = True
        except Exception as e:
            err_msg = str(e)
            try:
                # This will use a direct query, still returning Polars
                fallback_df = duck_conn.execute("""
                    SELECT * 
                    FROM {}
                    WHERE gene_id = ?
                    LIMIT {}
                """.format(selected_table, max_rows), [actual_gene_id]).pl()
                df = fallback_df
                data_loaded = True
                using_polars = True
                err_msg = "Used direct query fallback (no metadata join)"
            except Exception as final_e:
                raise Exception(f"All data retrieval methods failed. Last error: {str(final_e)}")
        
        # Get column counts for display
        col_count = len(df.columns)
            
        # Check if the metadata columns are present to determine if join worked
        metadata_columns = ["diagnosis", "age", "pmi", "sex", "apoe", "braak_tangle_score", "mapping_rate", "tin_median", "rin"]
        has_metadata = any(col in df.columns for col in metadata_columns)
            
        # Show metadata about the query result
        status_messages = []
        if not has_metadata:
            status_messages.append(html.P(
                "Note: Could not join with metadata table. Showing raw count data.",
                style={"color": "#ff9800", "font-style": "italic"}
            ))
        
        if row_count > max_rows:
            status_messages.append(html.P(
                f"Showing data for {row_count} total rows",
                style={"color": "#4CAF50", "font-weight": "bold", "font-size": "0.9rem"}
            ))
            
        if err_msg:
            status_messages.append(html.P(
                f"Warning: {err_msg}",
                style={"color": "#ff9800", "font-style": "italic", "font-size": "0.8rem"}
            ))
        
        # Return info and visualization container (without the table)
        return html.Div([
            html.H4(f"Table: {selected_table}", 
                   style={"color": "#333333", "margin-top": "1rem"}),
            html.P(f"Gene: {gene_name} ({actual_gene_id})",
                  style={"color": "#666666", "font-weight": "bold"}),
            html.Div(status_messages),
            
            # Only keep the visualization section
            html.H5("Visualization:", 
                   style={"color": "#333333", "margin-top": "2rem"}),
            html.Div(id='gene-plot-container', 
                    style={
                        "background-color": "#ffffff",
                        "padding": "15px",
                        "border-radius": "5px",
                        "border": "1px solid rgba(0, 0, 0, 0.1)",
                        "box-shadow": "0 2px 4px rgba(0, 0, 0, 0.1)",
                        "width": "100%",
                        "height": "calc(100vh - 400px)",  # Dynamic height based on viewport
                        "min-height": "600px"  # Minimum height to ensure visibility
                    })
        ])
            
    except Exception as e:
        import traceback
        trace = traceback.format_exc()
        return html.Div([
            html.P(f"Error loading table: {str(e)}", 
                 style={"color": "#dc3545"}),  # Error in red
            html.Pre(trace, style={"color": "#dc3545", "font-size": "0.8rem"})
        ])

# Add a new callback for the plot
@app.callback(
    Output('gene-plot-container', 'children'),
    [Input('matrix-table-dropdown', 'value'),
     Input('search-input', 'value'),
     Input('metadata-checklist', 'value'),
     Input('log-transform-option', 'value'),
     Input('plot-style-option', 'value')]  # Add the plot style input
)
def update_gene_plot(selected_table, selected_gene, selected_metadata, log_transform, plot_style):
    if not selected_table or not selected_gene:
        return html.P("Select a gene and table to display the visualization.", 
                     style={"color": "#666666"})
    
    try:
        # Get gene info
        gene_info = duck_conn.execute("""
            SELECT gene_id, gene_name 
            FROM transcript_annotation 
            WHERE gene_id = ?
            LIMIT 1
        """, [selected_gene]).fetchone()
        
        if not gene_info:
            return html.P(f"Gene ID '{selected_gene}' not found for visualization.", 
                        style={"color": "#666666"})
        
        actual_gene_id, gene_name = gene_info
        
        # Get data with metadata - remove the limit to get all samples
        expression = get_gene_data_with_metadata(actual_gene_id, selected_table, with_polars=True, limit=None)
        
        # Get annotation data for this gene
        annotation_query = """
            SELECT * 
            FROM transcript_annotation 
            WHERE gene_id = ?
        """
        annotation = duck_conn.execute(annotation_query, [actual_gene_id]).pl()
        
        #############################################################
        # Process the annotation data
        annotation = RNApy.shorten_gaps(annotation)
        
        # Order transcripts by expression level - show top 10 expressed transcripts
        expression, annotation = order_transcripts_by_expression(
            annotation_df=annotation, 
            expression_df=expression, 
            expression_column="cpm_normalized_tmm",
            top_n=[0,5]  # Show top 10 transcripts by default
        )

        # Handle metadata selection for expression_hue
        if selected_metadata is None or len(selected_metadata) == 0:
            # No metadata selected, don't use any expression_hue
            expression_hue = None
        elif len(selected_metadata) == 1:
            # If only one metadata column is selected, use it directly
            expression_hue = selected_metadata[0]
        else:
            # If multiple columns are selected, create a combined column using polars methods
            combined_col_name = "combined_metadata"
            
            # Convert selected columns to strings and combine them
            # First create an expression that converts each column to string
            
            # Start with the first column
            combined_expr = pl.col(selected_metadata[0]).cast(pl.Utf8)
            
            # Add the rest of the columns with separator
            for col_name in selected_metadata[1:]:
                combined_expr = combined_expr + pl.lit(" | ") + pl.col(col_name).cast(pl.Utf8)
            
            # Create the new column
            expression = expression.with_columns([
                combined_expr.alias(combined_col_name)
            ])
            
            expression_hue = combined_col_name

        # Apply log transformation if selected
        if log_transform:
            # Create copies of the data columns with log transform
            # We need to add 1 to avoid log(0) issues
            for col in ["counts", "cpm_normalized_tmm"]:
                if col in expression.columns:
                    import numpy as np
                    # Create a new column with log transform
                    log_col = f"log_{col}"
                    expression = expression.with_columns([
                        (pl.col(col).add(1).log10()).alias(log_col)
                    ])
            
            # Use the log-transformed columns in the plot
            expression_columns = ["log_counts", "log_cpm_normalized_tmm", "relative_abundance"]
        else:
            # Use original columns
            expression_columns = ["counts", "cpm_normalized_tmm", "relative_abundance"]
        
        # Update the trace creation to use the correct columns
        trace_params = {
            "annotation": annotation,
            "expression_matrix": expression,
            "x_start": "rescaled_start",
            "x_end": "rescaled_end",
            "y": "transcript_id",
            "annotation_hue": "transcript_biotype",
            "hover_start": "start",
            "hover_end": "end",
            "expression_columns": expression_columns,
            "marker_size": 3,
            "arrow_size": 7,
            "expression_plot_style": plot_style  # Add the plot style parameter
        }
        
        # Only add expression_hue if it's not None
        if expression_hue is not None:
            trace_params["expression_hue"] = expression_hue
            
        traces = RNApy.make_traces(**trace_params)
        
        # Create appropriate subplot titles based on transformation
        if log_transform:
            subplot_titles = ["Transcript Structure", "Log Counts", "Log TMM", "Relative Abundance"]
        else:
            subplot_titles = ["Transcript Structure", "Counts", "TMM", "Relative Abundance"]
            
        fig = RNApy.make_plot(traces=traces, 
                    subplot_titles=subplot_titles,
                    width=1500,  
                    height=600,
                    boxgap=0.1,
                    boxgroupgap=0.1)
                    

        # Then update the layout to be responsive
        fig.update_layout(
            autosize=True,
            # Compact margins for smaller screens
            margin=dict(l=30, r=30, t=50, b=30),
            # Ensure text sizes adjust with resizing
            font=dict(
                size=10  # Smaller base font size that can scale up
            ),
            legend=dict(
                font=dict(size=10)
            )
        )
        #############################################################
        
        # Return the figure as a Dash Graph component
        return dcc.Graph(
            figure=fig,
            style={
                "height": "100%",
                "width": "100%",
                "max-height": "70vh",  # Limit maximum height to 70% of viewport height
                "overflow": "hidden"   # Prevent overflow
            },
            config={
                "responsive": True,
                "displayModeBar": True,
                "scrollZoom": True
            }
        )
        
    except Exception as e:
        import traceback
        trace = traceback.format_exc()
        
        # Try to get column names to help with debugging
        column_info = ""
        try:
            if 'expression' in locals() and expression is not None:
                column_info = f"Available columns: {', '.join(expression.columns)}"
        except:
            column_info = "Could not retrieve column names"
            
        return html.Div([
            html.P(f"Error creating visualization: {str(e)}", 
                  style={"color": "#dc3545"}),
            html.P(column_info, 
                  style={"color": "#dc3545", "font-size": "0.9rem"}),
            html.Pre(trace, style={"color": "#dc3545", "font-size": "0.8rem"})
        ])