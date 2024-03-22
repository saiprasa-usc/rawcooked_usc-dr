import os.path
import sys

import dotenv
from PyQt5.QtCore import QRunnable, pyqtSlot, QObject, pyqtSignal

dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)


class WorkerSignals(QObject):
    finished = pyqtSignal()
    error = pyqtSignal(tuple)
    progressMessage = pyqtSignal(str)


class WorkFolderGenerator(QRunnable):
    def __init__(self, root_path):
        super(WorkFolderGenerator, self).__init__()
        self.root_path = root_path
        self.signals = WorkerSignals()

    def set_root_path(self) -> None:
        """Sets the Environment Variable FILM_OPS as Root Work directory
        Checks if the path provided is valid and then sets that as the Root Working Directory
        """

        if not os.path.exists(self.root_path):
            print(f"Invalid ROOT PATH: {self.root_path} Does not exist")
            # TODO: Raise an exception and send it in the error signal
            sys.exit(1)
        dotenv.set_key(dotenv_file, 'FILM_OPS', self.root_path)
        print("Root Path Set to Environment")
        self.signals.progressMessage.emit("Root Path Set to Environment")

    def generate_global_log_files(self) -> None:
        """Creates the ROOT_PATH/logs folder with three .log files for dpx_assessment, dpx_post_rawcook, dox_rawcook
        """

        print("Generating the logs folder")
        self.signals.progressMessage.emit("Generating the logs folder")
        logs_path = os.path.join(self.root_path, 'logs/')
        if os.path.exists(logs_path):
            print("logs folder is already present")
            self.signals.progressMessage.emit("logs folder is already present")
            return

        os.mkdir(logs_path)
        log_files = ['dpx_assessment.log', 'dpx_post_rawcook.log', 'dpx_rawcook.log']
        for file_name in log_files:
            file_path = os.path.join(logs_path, file_name)
            if os.path.exists(file_path):
                continue
            with open(file_path, 'w+'):
                pass
        print("logs folder created with respective log files")
        self.signals.progressMessage.emit("logs folder created with respective log files")

    def generate_policy_folder(self) -> None:
        """Creates the ROOT_PATH/policy folder. All the .XML policies need to be present in this folder
        """

        print("Generating the policy folder")
        self.signals.progressMessage.emit("Generating the policy folder")
        policy_path = os.path.join(self.root_path, 'policy/')
        if os.path.exists(policy_path):
            print("policy folder is already present")
            self.signals.progressMessage.emit("policy folder is already present")
            return
        os.mkdir(policy_path)
        print("Policy folder created. Keep all the XML Policies here")
        self.signals.progressMessage.emit("Policy folder created. Keep all the XML Policies here")

    def generate_media_folder(self) -> None:
        """Creates the ROOT_PATH/media folder with the hierarchy defined in media_structure
        The name of the folders will be the keys in media structure
        If the value is None then there are no subdirectories
        """

        print("Generating the media folder")
        self.signals.progressMessage.emit("Generating the media folder")
        media_path = os.path.join(self.root_path, 'media/')
        if os.path.exists(media_path):
            print("media folder is already present")
            self.signals.progressMessage.emit("media folder is already present")
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
        self.create_directory(media_path, media_structure)
        print("media folder created")
        self.signals.progressMessage.emit("media folder created")

    def create_directory(self, root_path, structure: dict) -> None:
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
                self.create_directory(new_folder_path, structure.get(name))

    @pyqtSlot()
    def run(self):
        self.set_root_path()
        self.generate_global_log_files()
        self.generate_policy_folder()
        self.generate_media_folder()
        self.signals.finished.emit()


if __name__ == '__main__':
    work_folder_generator = WorkFolderGenerator("/home")
    work_folder_generator.set_root_path()
    work_folder_generator.generate_global_log_files()
    work_folder_generator.generate_policy_folder()
    work_folder_generator.generate_media_folder()
