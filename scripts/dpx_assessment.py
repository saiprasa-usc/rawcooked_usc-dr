import os
import shutil
import sys

from dotenv import load_dotenv

from utils import find_utils, shell_utils, logging_utils

# Load environment variables from .env file
load_dotenv()

SCRIPT_LOG = os.path.join(os.environ.get('FILM_OPS'), os.environ.get('DPX_SCRIPT_LOG'))
DPX_PATH = os.path.join(os.environ.get('FILM_OPS'), os.environ.get('DPX_ASSESS'))
PY3_LAUNCH = os.environ.get('PY3_ENV')
SPLITTING = os.environ.get('SPLITTING_SCRIPT_FILMOPS')
POLICY_PATH = os.path.join(os.environ.get('FILM_OPS'), os.environ.get('POLICY_DPX'))
DPX_TO_COOK_PATH = os.path.join(os.environ.get('FILM_OPS'), os.environ['DPX_COOK'])


class DpxAssessment:
    def __init__(self):
        self.logfile = os.path.join(SCRIPT_LOG, 'dpx_assessment.log')
        self.rawcooked_dpx_file = os.path.join(DPX_PATH, 'temp_rawcooked_dpx_list.txt')
        self.tar_dpx_file = os.path.join(DPX_PATH, 'temp_tar_dpx_list.txt')
        self.success_file = os.path.join(DPX_PATH, 'rawcook_dpx_success.log')
        self.failure_file = os.path.join(DPX_PATH, 'tar_dpx_failures.log')

        # Refresh temporary success/failure lists
        self.temp_files = [self.rawcooked_dpx_file, self.tar_dpx_file]

    def process(self):
        """Initiates the workflow

        Checks if DPX sequences are present in the DXP_PATH and creates the temporary files needed for the workflow.
        Exits the script if there are no DPX files in the path specified in DPX_PATH
        """

        # Check for DPX sequences in path before script launch
        if not os.listdir(DPX_PATH):  # Make try catch
            print("No files available for encoding, script exiting")
            sys.exit(1)

        if not os.path.exists(self.success_file):
            with open(self.success_file, 'w+'):
                pass

        if not os.path.exists(self.failure_file):
            with open(self.failure_file, 'w+'):
                pass

        logging_utils.log(self.logfile, "\n============= DPX Assessment workflow START =============\n")

        # Creating temporary files from the temp_files list
        for file_name in self.temp_files:
            with open(file_name, 'w') as f:
                pass
            print(f"Created file: {file_name}")

    # TODO: Fix depth issue and remove Luma and 4k checks
    def check_mediaconch_policy(self):
        """Checks if a single files in each folder matches the mediaconch policies

        Randomly retrieves a single DPX file in each folder, runs mediaconch check and generates metadata files

        Recursively traverse through each sequence folder until the depth at which it finds a .dpx file
        Stores the root folder path and the path of the randomly chosen .dpx file as key value pairs in dpx_to_check
        Skips a sequence if the same absolute path is already present in rawcook_dpx_success.log or tar_dpx_failures.log
        """

        dpx_to_check = {}
        for seq in os.listdir(DPX_PATH):
            seq_path = os.path.join(DPX_PATH, seq)
            if os.path.isfile(seq_path) and not seq.endswith('.dpx'):
                continue

            queued_pass = find_utils.find_in_logs(os.path.join(DPX_PATH, "rawcook_dpx_success.log"), seq_path)
            queued_fail = find_utils.find_in_logs(os.path.join(DPX_PATH, "tar_dpx_failures.log"), seq_path)

            if queued_pass or queued_fail:
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

        for seq in dpx_to_check.keys():
            dpx = dpx_to_check.get(seq)
            logging_utils.log(self.logfile, f"Metadata file creation has started for: {dpx}")
            check = shell_utils.check_mediaconch_policy(POLICY_PATH, dpx)
            check_str = check.stdout.decode()
            if check_str.startswith("pass!"):
                with open(self.rawcooked_dpx_file, 'a') as file:
                    file.write(f"{seq}\n")
            else:
                logging_utils.log(self.logfile,
                                  f"FAIL: {dpx} DOES NOT CONFORM TO MEDIACONCH POLICY. Adding to "
                                  f"tar_dpx_failures_list.txt")
                logging_utils.log(self.logfile, check_str)
                with open(self.tar_dpx_file, 'a') as file:
                    file.write(f"{seq}\n")

    def move_passed_files(self):
        """Moves the processed and passed DPX sequences into the dpx_to_cook folder

        Collects all the folder (DPX sequence) names from the temp_rawcooked_dpx_list.txt
        Then moves them into dpx_to_cook folder

        Once all the passed files are moved, the remaining are the files that failed mediaconch policies
        """

        dpx_folders_to_move = []
        with open(self.rawcooked_dpx_file, 'r') as file:
            sorted_paths = file.read().splitlines()
            dpx_folders_to_move.extend(sorted_paths)

        for file_path in dpx_folders_to_move:
            shutil.move(file_path, DPX_TO_COOK_PATH)

    def log_success_failure(self):
        # Append latest pass/failures to movement logs
        with open(self.success_file, 'a') as target:
            with open(self.rawcooked_dpx_file, 'r') as source:
                target.write(source.read())

        with open(self.failure_file, 'a') as target:
            with open(self.tar_dpx_file, 'r') as source:
                target.write(source.read())

    def clean(self):
        """Concludes the workflow

        Moves the already processed folder containing all the DXP sequences from dpx_to_assess to dpx_to_cook
        and also deletes the temporary files created during the workflow
        """

        # Clean up temporary files
        for file_name in self.temp_files:
            if os.path.exists(file_name):
                os.remove(file_name)
                print(f"Deleted file: {file_name}")
            else:
                print(f"File not found: {file_name}")

    def execute(self):
        """Executes the workflow step by step as:

        1. process(): Checks if .dpx files are present in the input folder and creates temporary files
        2. check_mediaconch_policy(): Check a randomly chosen .dpx file from each sequence against mediaconch policies
        3. prepare_for_splitting(): Prepare files for splitting
        4. split(): Splits the files
        5. log_success_failure(): Logs the success or failure status
        6. clean(): Cleans up the temporary files
        """

        # TODO: Implement error handling mechanisms
        self.process()
        # TODO: Fix depth issue
        self.check_mediaconch_policy()

        self.move_passed_files()
        self.log_success_failure()
        self.clean()


if __name__ == '__main__':
    dpx_assessment = DpxAssessment()
    dpx_assessment.execute()
