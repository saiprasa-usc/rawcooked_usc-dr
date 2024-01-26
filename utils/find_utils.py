import os


def find_files(directory, depth):
    for root, dirs, files in os.walk(directory):
        current_depth = root[len(directory) + len(os.sep):].count(os.sep)
        if current_depth == depth:
            for file in files:
                file_path = os.path.join(root, file)
                print(file_path)


def find_directories(folder_name, target_depth):
    """Function to list all the directories at a particular dept starting from a root path

    @param folder_name: The root path from which we will start exploring
    @param target_depth: The directory level depth till which the search will go
    @return: A list of folders that are present in the leaf level
    """

    folders_at_depth = []

    for root, dirs, files in os.walk(folder_name):
        current_depth = root[len(folder_name) + len(os.path.sep):].count(os.path.sep)
        if current_depth == target_depth - 1:
            folders_at_depth.append(root)

    return folders_at_depth


def find_in_logs(file_path, search_text):
    """Function to check if a keyword is present in a file or not

    @param file_path: Complete path of the file in which it will search
    @param search_text: The search keyword
    @return: Boolean True if found else False
    """

    with open(file_path, 'r') as file:
        for line in file:
            if search_text in line:
                return True
    return False
