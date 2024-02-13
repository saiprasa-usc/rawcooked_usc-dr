import os.path
import sys

import dotenv

dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)

# Take user input
ROOT_PATH = '/home/tnandi/Desktop/rawcook_input/'


def set_root_path():
    if not os.path.exists(ROOT_PATH):
        print(f"Invalid ROOT PATH: {ROOT_PATH} Does not exist")
        sys.exit(1)
    dotenv.set_key(dotenv_file, 'FILM_OPS', ROOT_PATH)
    print("Root Path Set to Environment")


def generate_global_log_files():
    print("Generating the logs folder")
    logs_path = os.path.join(ROOT_PATH, 'logs/')
    if not os.path.exists(logs_path):
        os.mkdir(logs_path)
    log_files = ['dpx_assessment.log', 'dpx_post_rawcook.log', 'dpx_rawcook.log']
    for file_name in log_files:
        file_path = os.path.join(logs_path, file_name)
        if os.path.exists(file_path):
            continue
        with open(file_path, 'w+'):
            pass
    print("Logs folder created with respective log files")


def generate_policy_folder():
    print("Generating the Policy folder")
    policy_path = os.path.join(ROOT_PATH, 'policy/')
    if not os.path.exists(policy_path):
        os.mkdir(policy_path)
    print("Policy folder created. Keep all the XML Policies here")


def generate_media_folders():
    print("Generating the media folder")
    media_path = os.path.join(ROOT_PATH, 'media/')
    if not os.path.exists(media_path):
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


def create_directory(root_path, structure: dict):
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
    generate_media_folders()
