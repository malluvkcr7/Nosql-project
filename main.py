from systems import PostgreSQLSystem, MongoDBSystem, PigSystem
import re
import os

def clear_merge_timestamps():
    """Clear all merge timestamps at the start of execution"""
    systems = ['postgresql', 'mongodb', 'pig']
    for system in systems:
        try:
            os.remove(f'last_merge_{system}.json')
        except FileNotFoundError:
            pass
def parse_test_case(file_path):
    with open(file_path, 'r') as f:
        lines = [line.strip() for line in f if line.strip()]
    
    commands = []
    for line in lines:
        if ',' in line:
            # Timestamped command
            parts = line.split(',', 1)
            commands.append(('TIMESTAMPED', parts[0].strip(), parts[1].strip()))
        else:
            # Merge command (no timestamp)
            commands.append(('MERGE', None, line.strip()))
    return commands
def execute_command(command, systems):
    cmd_type, timestamp, cmd = command
    
    try:
        if cmd.startswith(('POSTGRESQL.', 'MONGODB.', 'PIG.')):
            system, operation = cmd.split('.', 1)
            system = system.lower()
            system_obj = systems[system]
            
            if operation.startswith('GET('):
                match = re.match(r'GET\((\w+),(\w+)\)', operation)
                if match:
                    student_id, course_id = match.groups()
                    grade = system_obj.get(student_id, course_id)
                    print(f"{cmd} => {grade}")
                else:
                    print(f"Invalid GET command: {cmd}")
            
            elif operation.startswith('SET('):
                match = re.match(r'SET\(\((\w+),(\w+)\),\s*(\w+)\)', operation)
                if match:
                    student_id, course_id, grade = match.groups()
                    system_obj.set(student_id, course_id, grade)
                    print(f"Executed: {cmd}")
                else:
                    print(f"Invalid SET command: {cmd}")
            
            elif operation.startswith('MERGE('):
                match = re.match(r'MERGE\((\w+)\)', operation)
                if match:
                    other_system = match.group(1).lower()
                    system_obj.merge(other_system)
                    print(f"Executed: {cmd}")
                else:
                    print(f"Invalid MERGE command: {cmd}")
            
            else:
                print(f"Unknown operation: {operation}")
        else:
            print(f"Invalid command format: {cmd}")
    except Exception as e:
        print(f"Error executing command '{cmd}': {str(e)}")
def main():
    # Clear any existing merge timestamps
    clear_merge_timestamps()
    
    # Initialize all systems
    systems = {
        'postgresql': PostgreSQLSystem(),
        'mongodb': MongoDBSystem(),
        'pig': PigSystem()
    }
    
    # Parse test cases
    test_case_file = 't3.in'
    commands = parse_test_case(test_case_file)
    
    # Execute all commands in order
    for command in commands:
        execute_command(command, systems)
if __name__ == "__main__":
    main()