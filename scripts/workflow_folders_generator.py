import os.path
import sys

import dotenv

dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)

# Take user input
ROOT_PATH = r'/home/test_again'


def set_root_path() -> None:
    """Sets the Environment Variable FILM_OPS as Root Work directory
    Checks if the path provided is valid and then sets that as the Root Working Directory
    """

    if not os.path.exists(ROOT_PATH):
        print(f"Invalid ROOT PATH: {ROOT_PATH} Does not exist")
        sys.exit(1)
    dotenv.set_key(dotenv_file, 'FILM_OPS', ROOT_PATH)
    print("Root Path Set to Environment")


def generate_global_log_files() -> None:
    """Creates the ROOT_PATH/logs folder with three .log files for dpx_assessment, dpx_post_rawcook, dox_rawcook
    """

    print("Generating the logs folder")
    logs_path = os.path.join(ROOT_PATH, 'logs/')
    if os.path.exists(logs_path):
        print('logs folder is already present')
        return

    os.mkdir(logs_path)
    log_files = ['dpx_assessment.log', 'dpx_post_rawcook.log', 'dpx_rawcook.log']
    for file_name in log_files:
        file_path = os.path.join(logs_path, file_name)
        if os.path.exists(file_path):
            continue
        with open(file_path, 'w+'):
            pass
    print("Logs folder created with respective log files")


def generate_policy_folder() -> None:
    """Creates the ROOT_PATH/policy folder. All the .XML policies need to be present in this folder
    """

    print("Generating the Policy folder")
    policy_path = os.path.join(ROOT_PATH, 'policy/')
    if os.path.exists(policy_path):
        print('policy folder is already present')
        return
    os.mkdir(policy_path)
    print("Policy folder created. Keep all the XML Policies here")


def generate_media_folder() -> None:
    """Creates the ROOT_PATH/media folder with the hierarchy defined in media_structure
    The name of the folders will be the keys in media structure
    If the value is None then there are no subdirectories
    """

    print("Generating the media folder")
    media_path = os.path.join(ROOT_PATH, 'media/')
    if os.path.exists(media_path):
        print("media folder is already present")
        return
    os.mkdir(media_path)
    media_structure = {
        'encoding': {
            'dpx_for_review': {
                'post_rawcook_fails': {
                    'mkv_files': None,
                    'rawcook_output_logs': None
                }
            },
            'dpx_to_assess': None,
            'rawcooked': {
                'dpx_to_cook': None,
                'dpx_to_cook_v2': None,
                'encoded': {
                    'mkv_cooked': None
                }
            }
        }
    }
    create_directory(media_path, media_structure)
    print("Media folder created")


def create_directory(root_path, structure: dict) -> None:
    """Recursively creates a directory structure
    Takes a root path and a dictionary where keys are directory name and values are subdirectory names
    If the value is none, then there are no subdirectory
    Starts DFS from the root_path and goes on creating directory hierarchy
    """

    if structure is None or len(structure) == 0:
        return
    for name in structure.keys():
        new_folder_path = os.path.join(root_path, name)
        if not os.path.exists(new_folder_path):
            os.mkdir(new_folder_path)
        if structure.get(name) is not None:
            create_directory(new_folder_path, structure.get(name))


if __name__ == '__main__':
    set_root_path()
    generate_global_log_files()
    generate_policy_folder()
    generate_media_folder()
