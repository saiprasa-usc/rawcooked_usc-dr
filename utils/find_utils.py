import os

def find_files(directory, depth):
    for root, dirs, files in os.walk(directory):
        current_depth = root[len(directory) + len(os.sep):].count(os.sep)
        if current_depth == depth:
            for file in files:
                file_path = os.path.join(root, file)
                print(file_path)

def find_directories(folder_name, target_depth):
    folders_at_depth = []
    
    for root, dirs, files in os.walk(folder_name):
        current_depth = root[len(folder_name) + len(os.sep):].count(os.sep)
        
        if current_depth == target_depth - 1:
            folders_at_depth.append(root)
    
    return folders_at_depth

def find_in_logs(file_path, search_text):
    with open(file_path, 'r') as file:
        for line in file:
            if search_text in line:
                return True
    return False