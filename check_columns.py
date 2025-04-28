from sqlalchemy import create_engine, text
import pandas as pd
import os

# Get DATABASE_URL from environment (Heroku provides this) or use local config
DATABASE_URL = os.environ.get('DATABASE_URL')

if DATABASE_URL:
    # Heroku's DATABASE_URL starts with postgres://, but SQLAlchemy requires postgresql://
    if DATABASE_URL.startswith('postgres://'):
        DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
    engine = create_engine(DATABASE_URL)
    print("Using Heroku PostgreSQL database")
else:
    # Local database fallback
    engine = create_engine("postgresql+psycopg2://postgres:isoforms@localhost:5432/ad_dash_app")
    print("Using local PostgreSQL database")

with engine.connect() as conn:
    # Get metadata columns
    meta_df = pd.read_sql("SELECT * FROM metadata LIMIT 1", conn)
    print("Metadata columns:", meta_df.columns.tolist())
    
    # Get transcript data columns
    transcript_df = pd.read_sql("SELECT * FROM total_transcript_data LIMIT 1", conn)
    print("Transcript columns:", transcript_df.columns.tolist()) 