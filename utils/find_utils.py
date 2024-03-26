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


def find_dpx_folder_from_sequence(dpx_folder_path) -> dict:
    """Function to find the nested folder containing only dpx_sequences

    :param dpx_folder_path: folder to check for dpx
    :return: dictionary with root folder and dpx folder path
    """
    dpx_sequence = {}
    for seq in os.listdir(dpx_folder_path):
        seq_path = os.path.join(dpx_folder_path, seq)
        if os.path.isfile(seq_path) and not seq.endswith('.dpx'):
            continue

        for dir_path, dir_names, file_names in os.walk(seq_path):
            if len(file_names) == 0:
                continue
            for file_name in file_names:
                if file_name.endswith('.dpx'):
                    dpx_sequence[seq_path] = dir_path
                    break

    return dpx_sequence
