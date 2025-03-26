from sqlalchemy import create_engine, text, inspect
import pandas as pd
import os
from pathlib import Path
import time
import sys
import gc


# Make the directory the script is in the working directory
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

db_url = "postgresql+psycopg2://postgres:isoforms@localhost:5432/ad_dash_app"

engine = create_engine(db_url, pool_size=5, max_overflow=10)

# Temporary placeholder for backward compatibility
duck_conn = None

try:
    with engine.connect() as connection:
        # This query returns the name of the current database.
        result = connection.execute(text("SELECT current_database();"))
        db_name = result.fetchone()[0]
        print(f"Successfully connected to the database: {db_name}")
        
        # Drop all existing tables to start with a clean slate
        connection.execute(text("DROP SCHEMA public CASCADE;"))
        connection.execute(text("CREATE SCHEMA public;"))
        connection.execute(text("GRANT ALL ON SCHEMA public TO postgres;"))
        connection.execute(text("GRANT ALL ON SCHEMA public TO public;"))
        
        # Verify that tables were actually cleared
        result = connection.execute(text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """))
        remaining_tables = result.fetchall()
        
        if not remaining_tables:
            print("Confirmed: All existing tables have been cleared from the database")
        else:
            table_names = [table[0] for table in remaining_tables]
            print(f"Warning: Some tables still exist: {', '.join(table_names)}")
except Exception as e:
    print("Failed to connect to the database or clear tables:", e)


# Open files in ../raw_data/ using pandas
raw_data_dir = Path("../raw_data/")
processed_count = 0

if raw_data_dir.exists():
    # Recursively find all files in raw_data directory and its subdirectories
    for file_path in raw_data_dir.glob("**/*"):
        if file_path.is_file():
            try:
                print(f"Loading file: {file_path}")
                start_time = time.time()
                
                # Create table name from file name without extension
                table_name = file_path.stem.lower()
                
                # For transcript_annotation.tsv, we need to handle the seqnames column differently
                if table_name == "transcript_annotation":
                    ## Define dtypes for the table
                    dtypes = {'gene_id': str, 'gene_name': str, 'transcript_id': str, 'transcript_name': str, 
                             'transcript_biotype': str, 'seqnames': str, 'strand': str, 'type': str, 
                             'start': int, 'end': int, 'exon_number': int}
                    
                    # Read and process file in chunks with specified dtypes
                    chunk_size = 1000000
                    for i, chunk in enumerate(pd.read_csv(file_path, sep="\t", chunksize=chunk_size, dtype=dtypes)):
                        if i == 0:
                            # First chunk - create table
                            chunk.to_sql(table_name, engine, if_exists='replace', index=False)
                            print(f"Created table '{table_name}' with first chunk of data")
                        else:
                            # Subsequent chunks - append to table
                            chunk.to_sql(table_name, engine, if_exists='append', index=False)
                            print(f"Appended chunk {i+1} to table '{table_name}'")
                        
                        # Clear memory
                        del chunk
                        gc.collect()
                else:
                    # For other files, use the original approach
                    chunk_size = 1000000
                    for i, chunk in enumerate(pd.read_csv(file_path, sep="\t", chunksize=chunk_size, low_memory=False)):
                        if i == 0:
                            # First chunk - create table
                            chunk.to_sql(table_name, engine, if_exists='replace', index=False)
                            print(f"Created table '{table_name}' with first chunk of data")
                        else:
                            # Subsequent chunks - append to table
                            chunk.to_sql(table_name, engine, if_exists='append', index=False)
                            print(f"Appended chunk {i+1} to table '{table_name}'")
                        
                        # Clear memory
                        del chunk
                        gc.collect()
                
                print(f"Successfully loaded all data into table '{table_name}'")
                
                # Identify columns to index based on specific requirements
                columns_to_index = []
                
                # Get column names without loading entire dataframe
                with engine.connect() as conn:
                    result = conn.execute(text(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"))
                    columns = [row[0] for row in result.fetchall()]
                
                # Check for gene_id
                if "gene_id" in columns:
                    columns_to_index.append("gene_id")
                
                # Check for gene_name if gene_id exists
                if "gene_id" in columns and "gene_name" in columns:
                    columns_to_index.append("gene_name")
                
                # Check for rsid
                if "rsid" in columns:
                    columns_to_index.append("rsid")
                
                # Add indexes on the identified columns
                if columns_to_index:
                    for col in columns_to_index:
                        try:
                            # Create a new connection for each index creation to avoid transaction blocks
                            # CREATE INDEX CONCURRENTLY cannot run inside a transaction block
                            index_name = f"idx_{table_name}_{col}"
                            with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                                conn.execute(text(f'CREATE INDEX CONCURRENTLY {index_name} ON "{table_name}" ("{col}");'))
                                print(f"Created index on column '{col}' for table '{table_name}'")
                                
                                # Verify the index was created
                                index_check_query = text(f"""
                                SELECT indexname 
                                FROM pg_indexes 
                                WHERE tablename = '{table_name}' 
                                AND indexname = '{index_name}'
                                """)
                                
                                result = conn.execute(index_check_query)
                                index_exists = result.fetchone()
                                
                                if index_exists:
                                    print(f"Verified index '{index_name}' was successfully created")
                                else:
                                    print(f"WARNING: Index '{index_name}' was not found after creation attempt")
                                    
                        except Exception as e:
                            print(f"Failed to create index on column '{col}': {e}")
                else:
                    print(f"No indexes created for table '{table_name}' as it doesn't contain gene_id, gene_name, or rsid columns")
                
                elapsed_time = time.time() - start_time
                print(f"Processed {table_name} in {elapsed_time:.2f} seconds")
                processed_count += 1
                
            except Exception as e:
                print(f"Failed to load {file_path}: {e}")
    
    print(f"Loaded {processed_count} files into database tables with indexes")
else:
    print("Raw data directory not found at ./data/raw_data")