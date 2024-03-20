import os
import shutil
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

from utils import find_utils, shell_utils, logging_utils

# Load environment variables from .env file
load_dotenv()

PY3_LAUNCH = os.environ.get('PY3_ENV')
SCRIPT_LOG = os.path.join(os.environ.get('FILM_OPS'), os.environ.get('DPX_SCRIPT_LOG'))
DPX_PATH = os.path.join(os.environ.get('FILM_OPS'), os.environ.get('DPX_ASSESS'))
POLICY_PATH = os.path.join(os.environ.get('FILM_OPS'), os.environ.get('POLICY_DPX'))
DPX_TO_COOK_PATH = os.path.join(os.environ.get('FILM_OPS'), os.environ['DPX_COOK'])
DPX_TO_COOK_V2_PATH = os.path.join(os.environ.get('FILM_OPS'), os.environ['DPX_COOK_V2'])
DPX_FOR_REVIEW_PATH = os.path.join(os.environ.get('FILM_OPS'), os.environ['DPX_REVIEW'])

SCRIPT_LOG = r'{}'.format(SCRIPT_LOG)
DPX_PATH = r'{}'.format(DPX_PATH)
POLICY_PATH = r'{}'.format(POLICY_PATH)
DPX_TO_COOK_PATH = r'{}'.format(DPX_TO_COOK_PATH)
DPX_TO_COOK_V2_PATH = r'{}'.format(DPX_TO_COOK_V2_PATH)
DPX_FOR_REVIEW_PATH = r'{}'.format(DPX_FOR_REVIEW_PATH)


class DpxAssessment:
    def __init__(self):
        # Log files
        self.logfile = os.path.join(SCRIPT_LOG, 'dpx_assessment.log')
        self.success_file = os.path.join(DPX_PATH, 'rawcook_dpx_success.log')
        self.failure_file = os.path.join(DPX_PATH, 'tar_dpx_failures.log')
        self.review_file = os.path.join(DPX_PATH, 'review_dpx_failures.log')
        self.rawcooked_v2_file = os.path.join(DPX_PATH, 'rawcook_dpx_v2.log')

        # Temporary .txt files
        self.temp_rawcooked_dpx_file = os.path.join(DPX_PATH, 'temp_rawcooked_dpx_list.txt')
        self.temp_rawcooked_v2_dpx_file = os.path.join(DPX_PATH, 'temp_rawcooked_v2_dpx_list.txt')
        self.temp_tar_dpx_file = os.path.join(DPX_PATH, 'temp_tar_dpx_list.txt')
        self.temp_review_dpx_file = os.path.join(DPX_PATH, 'temp_review_dpx.txt')

        # List of the above temp files needed for creation and deletion
        self.temp_files = [
            self.temp_rawcooked_dpx_file, self.temp_rawcooked_v2_dpx_file,
            self.temp_tar_dpx_file, self.temp_review_dpx_file
        ]

    def process(self) -> None:
        """Initiates the workflow

        Checks if DPX sequences are present in the DXP_PATH and creates the temporary files needed for the workflow.
        Exits the script if there are no DPX files in the path specified in DPX_PATH
        """

        # Check for DPX sequences in path before script launch
        if not os.listdir(DPX_PATH):  # Make try catch
            print("No files available for encoding, script exiting")
            sys.exit(1)

        if not os.path.exists(self.logfile):
            with open(self.logfile, 'w+'):
                pass

        if not os.path.exists(self.success_file):
            with open(self.success_file, 'w+'):
                pass

        if not os.path.exists(self.failure_file):
            with open(self.failure_file, 'w+'):
                pass

        if not os.path.exists(self.review_file):
            with open(self.review_file, 'w+'):
                pass

        if not os.path.exists(self.rawcooked_v2_file):
            with open(self.rawcooked_v2_file, 'w+'):
                pass

        logging_utils.log(self.logfile, "\n============= DPX Assessment workflow START =============\n")

        # Creating temporary files from the temp_files list
        for file_name in self.temp_files:
            with open(file_name, 'w+'):
                pass
            print(f"Created file: {file_name}")

    def find_dpx_to_check(self) -> dict:
        """Randomly retrieves a DPX file in each folder and creates a dictionary with <root_folder, dpx_file> pairs

        Recursively traverse through each sequence folder until the depth at which it finds a .dpx file
        Stores the root folder path and the path of the randomly chosen .dpx file as key value pairs in dpx_to_check
        Skips a sequence if the same absolute path is already present in rawcook_dpx_success.log or tar_dpx_failures.log
        """
        dpx_to_check = {}
        for seq in os.listdir(DPX_PATH):
            seq_path = os.path.join(DPX_PATH, seq)
            if os.path.isfile(seq_path) and not seq.endswith('.dpx'):
                continue

            # checks in the logs if the sequence has already been processed
            queued_pass = find_utils.find_in_logs(self.success_file, seq_path)
            queued_fail = find_utils.find_in_logs(self.failure_file, seq_path)
            queued_review = find_utils.find_in_logs(self.review_file, seq_path)
            queued_v2 = find_utils.find_in_logs(self.rawcooked_v2_file, seq_path)

            if queued_pass or queued_fail or queued_review or queued_v2:
                logging_utils.log(self.logfile,
                                  f"SKIPPING DPX folder: {seq_path}, it has already been processed but has not "
                                  f"moved to correct processing path:")
                continue

            for dir_path, dir_names, file_names in os.walk(seq_path):
                if len(file_names) == 0:
                    continue
                for file_name in file_names:
                    if file_name.endswith('.dpx'):
                        dpx_to_check[seq_path] = os.path.join(dir_path, file_name)
                        break

        return dpx_to_check

    def check_gaps_and_v2(self, dpx_to_check: dict) -> list:
        """Executes Rawcooked to check if there is a missing .dpx in a sequence or if the sequence generates large
        reversibility file

        The incoherent sequences are removed from the dictionary as we do not need to execute mediaconch over them
        And the sequences with large reversibility file is added to a temp_rawcooked_v2_dpx_list.txt file
        Returns a list of the incoherent sequences for separating them later
        """
        sequences_to_review = []
        sequences_to_v2 = []
        for seq in dpx_to_check.keys():
            # Rawcooked should take a folder as input which contains only .dpx files and no other metadata file
            # The value of dpx_to_check dict is the path to a .dpx file which implies that the parent of this path is
            # The root dpx folder that rawcooked should take as input
            root_dpx_folder = Path(dpx_to_check.get(seq)).parent
            command = ['rawcooked', '--license', '004B159A2BDB07331B8F2FDF4B2F', '--check', '--no-encode', root_dpx_folder]
            logging_utils.log(self.logfile,
                              f"Checking for incoherent sequences and large reversibility file issue in {seq}")

            subprocess_logs = []
            # Note: By observation, output of Rawcooked is captured in stderr not in stdout
            with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as p:
                for line in p.stderr:
                    subprocess_logs.append(line)
                    print(line)
                for line in p.stdout:
                    subprocess_logs.append(line)
                    print(line)

            std_logs = ''.join(subprocess_logs)

            # Checks for incoherent sequences
            if std_logs.find('Warning: incoherent file names') != -1:
                logging_utils.log(self.logfile,
                                  f"FAIL: {seq} CONTAINS INCOHERENT SEQUENCES. Adding to "
                                  f"temp_review_dpx_list.txt")
                with open(self.temp_review_dpx_file, 'a') as file:
                    file.write(f"{seq}\n")
                sequences_to_review.append(seq)

            # Checks for sequences with large reversibility file
            if std_logs.find('Error: the reversibility file is becoming big') != -1:
                logging_utils.log(self.logfile,
                                  f"FAIL: {seq} REVERSIBILITY FILE IS TOO BIG. Adding to "
                                  f"temp_rawcooked_v2_dpx_list.txt")

                with open(self.temp_rawcooked_v2_dpx_file, 'a') as file:
                    file.write(f"{seq}\n")
                sequences_to_v2.append(seq)

        # We don't need to run mediaconch over these sequences so remove the entries
        for seq in sequences_to_review:
            dpx_to_check.pop(seq)
        for seq in sequences_to_v2:
            dpx_to_check.pop(seq)

        return sequences_to_review

    def check_mediaconch_policy(self, dpx_to_check: dict) -> None:
        """Checks if the dpx files passed as parameters matches mediaconch policies
        """

        for seq in dpx_to_check.keys():
            dpx = dpx_to_check.get(seq)
            logging_utils.log(self.logfile, f"Metadata file creation has started for: {dpx}")
            check = shell_utils.check_mediaconch_policy(POLICY_PATH, dpx)
            check_str = check.stdout.decode()
            if check_str.startswith("pass!"):
                with open(self.temp_rawcooked_dpx_file, 'a') as file:
                    file.write(f"{seq}\n")
            else:
                logging_utils.log(self.logfile,
                                  f"FAIL: {dpx} DOES NOT CONFORM TO MEDIACONCH POLICY. Adding to "
                                  f"tar_dpx_failures_list.txt")
                logging_utils.log(self.logfile, check_str)
                with open(self.temp_tar_dpx_file, 'a') as file:
                    file.write(f"{seq}\n")

    def move_review_sequences(self, sequences_to_review: list) -> None:
        """Moves the sequences passed as parameter to the dpx_for_review folder
        """

        for seq in sequences_to_review:
            shutil.move(seq, DPX_FOR_REVIEW_PATH)

    def move_v2_sequences(self) -> None:
        """Moves the failed filed due to large reversibility file into dpx_to_cook_v2 folder

        Collects all the sequences from the temp_rawcook_v2_dpx_list.txt then moves them into dpx_to_cook_v2 folder
        """

        dpx_folders_to_move = []
        with open(self.temp_rawcooked_v2_dpx_file, 'r') as file:
            sorted_paths = file.read().splitlines()
            dpx_folders_to_move.extend(sorted_paths)

        for file_path in dpx_folders_to_move:
            shutil.move(file_path, DPX_TO_COOK_V2_PATH)

    def move_passed_sequences(self) -> None:
        """Moves the processed and passed DPX sequences into the dpx_to_cook folder

        Collects all the folder (DPX sequence) names from the temp_rawcooked_dpx_list.txt
        Then moves them into dpx_to_cook folder

        Once all the passed files are moved, the remaining are the files that failed mediaconch policies
        """

        dpx_folders_to_move = []
        with open(self.temp_rawcooked_dpx_file, 'r') as file:
            sorted_paths = file.read().splitlines()
            dpx_folders_to_move.extend(sorted_paths)

        for file_path in dpx_folders_to_move:
            shutil.move(file_path, DPX_TO_COOK_PATH)

    def log_success_failure(self) -> None:
        """Takes the value from the temporary files and stores them into the respective .log files
        """

        with open(self.success_file, 'a') as target:
            with open(self.temp_rawcooked_dpx_file, 'r') as source:
                target.write(source.read())

        with open(self.failure_file, 'a') as target:
            with open(self.temp_tar_dpx_file, 'r') as source:
                target.write(source.read())

        with open(self.review_file, 'a') as target:
            with open(self.temp_review_dpx_file, 'r') as source:
                target.write(source.read())

        with open(self.rawcooked_v2_file, 'a') as target:
            with open(self.temp_rawcooked_v2_dpx_file, 'r') as source:
                target.write(source.read())

    def clean(self) -> None:
        """Concludes the workflow by removing the temporary .txt files
        """

        # Clean up temporary files
        for file_name in self.temp_files:
            if os.path.exists(file_name):
                os.remove(file_name)
                print(f"Deleted file: {file_name}")
            else:
                print(f"File not found: {file_name}")

    def execute(self) -> None:
        """Executes the workflow step by step as:

        1. process(): Checks if .dpx files are present in the input folder and creates temporary files
        2. find_dpx_to_check(): Finds the dpx files at any depth and returns a dict with <root_path, file_path> pairs
        3. check_gaps(): It takes the dict and runs rawcooked to check if there is an incoherent sequence.
                         Removes the sequence from the dict
        4. check_mediaconch_policy(): Check a randomly chosen .dpx file from each sequence against mediaconch policies
        5. move_review_sequences(): Takes the list of incoherent sequences and moves them to dpx_for_review folder
        6. move_v2_sequences(): Takes the list of large reversibility file sequences and moves them to dpx_to_cook_v2
        7. move_passed_sequences(): Takes the list of all the passed sequences and moves them to dpx_to_cook folder
        8. log_success_failure(): Logs the success or failure status
        9. clean(): Cleans up the temporary files
        """

        # TODO: Implement error handling mechanisms
        self.process()

        dpx_to_check = self.find_dpx_to_check()
        sequences_to_review = self.check_gaps_and_v2(dpx_to_check)
        self.check_mediaconch_policy(dpx_to_check)
        if len(sequences_to_review) > 0:
            self.move_review_sequences(sequences_to_review)
        self.move_v2_sequences()
        self.move_passed_sequences()
        self.log_success_failure()
        self.clean()


if __name__ == '__main__':
    dpx_assessment = DpxAssessment()
    dpx_assessment.execute()
