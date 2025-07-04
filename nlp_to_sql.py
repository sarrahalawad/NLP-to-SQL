import openai
import sqlite3
import pandas as pd
from typing import Optional, Dict, List
#!pip3 install openai==0.28

# Defining the converter that takes NLP and outputs SQL

class NLPtoSQL:

    def __init__(self, api_key: str, database_path: str):
        """Initialize the NLP to SQL converter.
        
        Args:
            api_key (str): OpenAI API key
            database_path (str): Path to SQLite database
        """
        self.api_key = api_key
        openai.api_key = api_key
        self.db_path = database_path
        self.conn = sqlite3.connect(database_path)
        self.cursor = self.conn.cursor()
        
    def get_table_schema(self) -> str:
        """Get the schema of all tables in the database."""
        tables = self.cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        ).fetchall()
        
        schema = []
        for table in tables:
            table_name = table[0]
            columns = self.cursor.execute(f"PRAGMA table_info({table_name})").fetchall()
            columns_str = ', '.join([f"{col[1]} ({col[2]})" for col in columns])
            schema.append(f"Table: {table_name}\nColumns: {columns_str}\n")
        
        return '\n'.join(schema)
    
    def generate_sql(self, query: str) -> str:
        """Convert natural language query to SQL.
        
        Args:
            query (str): Natural language query
            
        Returns:
            str: Generated SQL query
        """
        schema = self.get_table_schema()
        prompt = f"""Given the following database schema:\n{schema}\n
        Convert this natural language query to SQL:\n{query}\n
        Return only the SQL query without any explanation."""
        
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a SQL expert. Generate SQL queries from natural language."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.0
        )
        
        return response.choices[0].message.content.strip()
    
    def execute_query(self, sql_query: str) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame.
        
        Args:
            sql_query (str): SQL query to execute
            
        Returns:
            pd.DataFrame: Query results
        """
        try:
            return pd.read_sql_query(sql_query, self.conn)
        except Exception as e:
            raise Exception(f"Error executing query: {str(e)}")
    
    def natural_query(self, query: str) -> pd.DataFrame:
        """Process natural language query and return results.
        
        Args:
            query (str): Natural language query
            
        Returns:
            pd.DataFrame: Query results
        """
        sql_query = self.generate_sql(query)
        return self.execute_query(sql_query)
    
    def close(self):
        """Close database connection."""
        self.conn.close()

# Test Scenario
def main():
    # add the OpenAI API key and db
    api_key = "api-key"
    db_path = "database.db"
    
    nlp_sql = NLPtoSQL(api_key, db_path)
    
    # Example test
    queries = [
        "Show me all users who made a purchase in the last month",
        "What are the top 5 most viewed products?",
        "Calculate the average transaction amount by category"
    ]
    
    try:
        for query in queries:
            print(f"\nNatural Language Query: {query}")
            result = nlp_sql.natural_query(query)
            print("\nResults:")
            print(result)
    finally:
        nlp_sql.close()

if __name__ == "__main__":
    main()



    