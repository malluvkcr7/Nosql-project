import pandas as pd
from pymongo import MongoClient
import psycopg2
import subprocess
import os

CSV_FILE = "student_course_grades.csv"

def load_mongo(df):
    client = MongoClient("mongodb://localhost:27017/")
    db = client["nosql_project"]
    collection = db["grades"]
    collection.drop()  # clear previous
    records = df.to_dict(orient="records")
    collection.insert_many(records)
    print("✅ Data loaded into MongoDB")

def load_postgresql(df):
    conn = psycopg2.connect(
        dbname="nosql_db",
        user="postgres",
        password="new_password",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS grades;")
    cur.execute("""
        CREATE TABLE grades (
            student_id TEXT,
            course_id TEXT,
            roll_no TEXT,
            email TEXT,
            grade TEXT,
            PRIMARY KEY (student_id, course_id)
        );
    """)
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO grades (student_id, course_id, roll_no, email, grade)
            VALUES (%s, %s, %s, %s, %s)
        """, tuple(row))
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Data loaded into PostgreSQL")

def load_pig(df):
    os.makedirs("pig_data", exist_ok=True)
    
    # 1. Save data to temporary CSV
    temp_csv = "pig_data/temp_load.csv"
    df.to_csv(temp_csv, index=False, header=False)
    
    # 2. Create Pig load script with directory cleanup
    load_script = f"""
    -- Delete output directory if it exists
    fs -rm -r -f pig_data/grades;
    
    -- Load from CSV
    raw_data = LOAD '{temp_csv}' USING PigStorage(',')
        AS (
            student_id:chararray,
            course_id:chararray,
            roll_no:chararray,
            email:chararray,
            grade:chararray
        );
    
    -- Store in Pig format
    STORE raw_data INTO 'pig_data/grades' USING PigStorage();
    """
    
    # 3. Execute load script
    try:
        with open('pig_data/load_script.pig', 'w') as f:
            f.write(load_script)
        
        result = subprocess.run(
            ['pig', '-x', 'local', 'pig_data/load_script.pig'],
            check=True,
            capture_output=True,
            text=True
        )
        print("✅ Data properly loaded into Pig storage")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"❌ Error loading data into Pig: {e}")
        print(f"Pig Error Output:\n{e.stderr}")
    finally:
        # Clean up temporary files
        if os.path.exists(temp_csv):
            os.remove(temp_csv)
        if os.path.exists('pig_data/load_script.pig'):
            os.remove('pig_data/load_script.pig')
def main():
    df = pd.read_csv(CSV_FILE)
    df.columns = ['student_id', 'course_id', 'roll_no', 'email', 'grade']  # normalize column names
    load_mongo(df)
    load_postgresql(df)
    load_pig(df)

if __name__ == "__main__":
    main()