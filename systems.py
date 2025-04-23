import psycopg2
from pymongo import MongoClient
import subprocess
import pandas as pd
from oplog import OperationLog
import os
from datetime import datetime
import re
import shutil

class PostgreSQLSystem:
    def __init__(self):
        self.oplog = OperationLog('postgresql')
        self.conn_params = {
            'dbname': 'nosql_db',
            'user': 'postgres',
            'password': 'new_password',
            'host': 'localhost',
            'port': '5432'
        }
        self.system_name = 'postgresql'
    
    def get_connection(self):
        return psycopg2.connect(**self.conn_params)
    
    def get(self, student_id, course_id):
        conn = self.get_connection()
        cursor = conn.cursor()
        query = """
            SELECT grade FROM grades 
            WHERE student_id = %s AND course_id = %s
        """
        cursor.execute(query, (student_id, course_id))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        self.oplog.add_operation('GET', (student_id, course_id))
        return result[0] if result else None
    
    def set(self, student_id, course_id, grade):
        conn = self.get_connection()
        cursor = conn.cursor()
        query = """
            UPDATE grades 
            SET grade = %s 
            WHERE student_id = %s AND course_id = %s
        """
        cursor.execute(query, (grade, student_id, course_id))
        conn.commit()
        cursor.close()
        conn.close()
        
        self.oplog.add_operation('SET', (student_id, course_id), grade)
    
    def merge(self, other_system_name):
        other_system_name = other_system_name.lower()
        other_oplog = OperationLog(other_system_name)
        other_ops = other_oplog.get_operations_since_merge(self.system_name.lower())
        
        # Apply operations in chronological order
        for op in sorted(other_ops, key=lambda x: x['timestamp']):
            if op['type'] == 'SET':
                student_id, course_id = op['key']
                grade = op['value']
                self.set(student_id, course_id, grade)
        
        # Record merge in both systems
        merge_time = datetime.now().isoformat()
        self.oplog.record_merge(other_system_name, merge_time)
        other_oplog.record_merge(self.system_name.lower(), merge_time)
class MongoDBSystem:
    def __init__(self):
        self.oplog = OperationLog('mongodb')
        self.client = MongoClient("mongodb://localhost:27017/")
        self.db = self.client["nosql_project"]
        self.collection = self.db["grades"]
        self.system_name = 'mongodb'
    
    def get(self, student_id, course_id):
        result = self.collection.find_one({
            'student_id': student_id,
            'course_id': course_id
        })
        
        self.oplog.add_operation('GET', (student_id, course_id))
        return result['grade'] if result else None
    
    def set(self, student_id, course_id, grade):
        self.collection.update_one(
            {'student_id': student_id, 'course_id': course_id},
            {'$set': {'grade': grade}},
            upsert=True
        )
        
        self.oplog.add_operation('SET', (student_id, course_id), grade)
    
    def merge(self, other_system_name):
        other_system_name = other_system_name.lower()
        other_oplog = OperationLog(other_system_name)
        other_ops = other_oplog.get_operations_since_merge(self.system_name.lower())
        
        for op in sorted(other_ops, key=lambda x: x['timestamp']):
            if op['type'] == 'SET':
                student_id, course_id = op['key']
                grade = op['value']
                self.set(student_id, course_id, grade)
        
        merge_time = datetime.now().isoformat()
        self.oplog.record_merge(other_system_name, merge_time)
        other_oplog.record_merge(self.system_name.lower(), merge_time)
class PigSystem:
    def __init__(self):
        self.oplog = OperationLog('pig')
        self.data_dir = 'pig_data/grades'
        self.system_name = 'pig'
        
        os.makedirs('pig_data', exist_ok=True)

        if not os.path.exists(os.path.join(self.data_dir, 'part-m-00000')):
            self._initialize_pig_storage()

    def _initialize_pig_storage(self):
        """Initialize empty Pig storage with proper schema"""
        with open('empty.csv', 'w') as f:
            f.write("")  # Empty CSV to simulate schema

        init_script = """
        data = LOAD 'empty.csv' USING PigStorage(',')
            AS (
                student_id:chararray,
                course_id:chararray,
                roll_no:chararray,
                email:chararray,
                grade:chararray
            );
        STORE data INTO 'pig_data/grades';
        """

        try:
            result = subprocess.run(
                ['pig', '-x', 'local', '-e', init_script],
                capture_output=True,
                text=True
            )
            if result.returncode != 0:
                print(f"Pig initialization error: {result.stderr}")
        finally:
            os.remove('empty.csv')

    def _execute_pig(self, script):
        """Run a Pig script and return its output"""
        script_path = 'pig_data/temp_script.pig'
        with open(script_path, 'w') as f:
            f.write(script)

        try:
            result = subprocess.run(
                ['pig', '-x', 'local', script_path],
                capture_output=True,
                text=True
            )
            if result.returncode != 0:
                raise Exception(f"Pig error: {result.stderr}")
            return result.stdout
        finally:
            os.remove(script_path)

    def get(self, student_id, course_id):
        """Retrieve grade for a specific student-course pair"""
        script = f"""
        data = LOAD '{self.data_dir}' USING PigStorage();
        filtered = FILTER data BY 
            $0 == '{student_id}' AND $1 == '{course_id}';
        limited = LIMIT filtered 1;
        DUMP limited;
        """

        try:
            output = self._execute_pig(script)
            match = re.search(r'\(([^)]+)\)', output)
            if match:
                fields = match.group(1).split(',')
                grade = fields[-1].strip()
                self.oplog.add_operation('GET', (student_id, course_id))
                return grade
            return None
        except Exception as e:
            print(f"Error in Pig GET: {str(e)}")
            return None

    
    def set(self, student_id, course_id, grade):
        try:
            os.makedirs('pig_data', exist_ok=True)
            use_empty_mode = not os.path.exists(self.data_dir)

            with open('empty.csv', 'w') as f:
                f.write("")  # Ensure dummy exists for Pig

            if use_empty_mode:
                # CASE 1: No grades exist yet — create new Pig data with one row
                script = f"""
                dummy = LOAD 'empty.csv' USING PigStorage() AS (a:chararray);
                new_records = FOREACH dummy GENERATE
                    '{student_id}',
                    '{course_id}',
                    '',
                    '',
                    '{grade}';
                STORE new_records INTO '{self.data_dir}' USING PigStorage();
                """
            else:
                # CASE 2: Existing data — load, update or append
                temp_dir = self.data_dir + "_temp"
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)

                script = f"""
                all_data = LOAD '{self.data_dir}' USING PigStorage();
                to_update = FILTER all_data BY 
                    $0 == '{student_id}' AND $1 == '{course_id}';
                others = FILTER all_data BY 
                    NOT ($0 == '{student_id}' AND $1 == '{course_id}');
                updated = FOREACH to_update GENERATE
                    '{student_id}', '{course_id}', $2, $3, '{grade}';
                dummy = LOAD 'empty.csv' USING PigStorage() AS (a:chararray);
                new_records = FOREACH dummy GENERATE
                    '{student_id}', '{course_id}', '', '', '{grade}';
                combined = UNION others, updated, new_records;
                STORE combined INTO '{temp_dir}' USING PigStorage();
                """

            # Run the Pig script
            self._execute_pig(script)

            # Replace the old data folder with the temp folder only if updating
            if not use_empty_mode:
                if os.path.exists(self.data_dir):
                    shutil.rmtree(self.data_dir)
                shutil.move(temp_dir, self.data_dir)

            # Log the operation
            self.oplog.add_operation('SET', (student_id, course_id), grade)

        except Exception as e:
            print(f"Error in Pig SET: {str(e)}")
        finally:
            if os.path.exists('empty.csv'):
                os.remove('empty.csv')


    def merge(self, other_system_name):
        """Merge changes from another system with proper conflict resolution"""
        other_oplog = OperationLog(other_system_name)
        other_ops = other_oplog.get_operations_since_merge(self.system_name)
        
        # Get our most recent SET operations for conflict resolution
        our_set_ops = {
            op['key']: op['timestamp'] 
            for op in reversed(self.oplog.operations) 
            if op['type'] == 'SET'
        }
        
        # Apply operations in chronological order
        for op in sorted(other_ops, key=lambda x: x['timestamp']):
            if op['type'] == 'SET':
                student_id, course_id = op['key']
                grade = op['value']
                op_time = op['timestamp']
                
                # Only apply if the operation is newer than our last SET for this key
                if (student_id, course_id) not in our_set_ops or op_time > our_set_ops[(student_id, course_id)]:
                    self.set(student_id, course_id, grade)
        
        # Record merge in both systems
        merge_time = datetime.now().isoformat()
        self.oplog.record_merge(other_system_name, merge_time)
        other_oplog.record_merge(self.system_name, merge_time)