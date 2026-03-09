
import os
from sqlalchemy import create_engine, inspect

def get_engine():
    db_user = os.getenv("DATA_DB_USER", "dw")
    db_password = os.getenv("DATA_DB_PASSWORD", "dw")
    db_host = os.getenv("DATA_DB_HOST", "postgres")
    db_port = os.getenv("DATA_DB_PORT", "5432")
    db_name = os.getenv("DATA_DB_NAME", "train_dw")
    
    connection_str = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    return create_engine(connection_str)

engine = get_engine()
inspector = inspect(engine)

print("--- Tables in dwh schema ---")
for table_name in inspector.get_table_names(schema='dwh'):
    print(f"Table: {table_name}")
    for column in inspector.get_columns(table_name, schema='dwh'):
        print(f"  - {column['name']} ({column['type']})")

print("\n--- Views in dwh schema ---")
for view_name in inspector.get_view_names(schema='dwh'):
    print(f"View: {view_name}")
    # Inspecting columns of views might need raw SQL or try get_columns if supported
    try:
        for column in inspector.get_columns(view_name, schema='dwh'):
            print(f"  - {column['name']} ({column['type']})")
    except:
        print("  (Cannot inspect view columns directly)")
