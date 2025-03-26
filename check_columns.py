from sqlalchemy import create_engine, text
import pandas as pd

# Create engine
engine = create_engine("postgresql+psycopg2://postgres:isoforms@localhost:5432/ad_dash_app")

with engine.connect() as conn:
    # Get metadata columns
    meta_df = pd.read_sql("SELECT * FROM metadata LIMIT 1", conn)
    print("Metadata columns:", meta_df.columns.tolist())
    
    # Get transcript data columns
    transcript_df = pd.read_sql("SELECT * FROM total_transcript_data LIMIT 1", conn)
    print("Transcript columns:", transcript_df.columns.tolist()) 