import os
import re
import shutil
import sys
from pathlib import Path

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
        self.rawcooked_dpx_file = os.path.join(DPX_PATH, 'rawcooked_dpx_list.txt')
        self.luma_4k_dpx_file = os.path.join(DPX_PATH, 'luma_4k_dpx_list.txt')
        self.tar_dpx_file = os.path.join(DPX_PATH, 'tar_dpx_list.txt')
        self.success_file = os.path.join(DPX_PATH, 'rawcook_dpx_success.log')
        self.failure_file = os.path.join(DPX_PATH, 'tar_dpx_failures.log')

        # Refresh temporary success/failure lists
        self.temp_files = [self.rawcooked_dpx_file, self.tar_dpx_file, self.luma_4k_dpx_file]

    def process(self):
        """Initiates the workflow

        Checks if DPX sequences are present in the DXP_PATH and creates the temporary files needed for the workflow.
        Exits the script if there are no DPX files in the path specified in DPX_PATH
        """

        # Check for DPX sequences in path before script launch
        if not os.listdir(DPX_PATH):  # Make try catch
            print("No files available for encoding, script exiting")
            sys.exit(1)
        else:
            logging_utils.log(self.logfile, "============= DPX Assessment workflow START =============")

        # Creating temporary files from the temp_files list
        for file_name in self.temp_files:
            with open(file_name, 'w') as f:
                pass
            print(f"Created file: {file_name}")

    # TODO: Fix depth issue and remove Luma and 4k checks
    def check_mediaconch_policy(self, depth=3):
        """Checks if the files in each folder matches the mediaconch policies

        Loop that retrieves single DPX file in each folder, runs Mediaconch check and generates metadata files
        Configured for three level folders: N_123456_01of01/scan01/dimensions/<dpx_seq>
        depth should be 3 (Needs to be configurable. Work in progress)
        """

        # TODO: Remove the Luma and 4k checking
        dirs = find_utils.find_directories(DPX_PATH, depth)
        # Checking mediaconch policy for first DPX file in each folder
        for dir_name in dirs:
            path = Path(dir_name)
            dpx = os.listdir(path)[0]
            # components = dir_name.split('/')
            dimensions = path.name
            scans = path.parent.name
            filename = path.parent.parent.name
            file_scan_name = os.path.join(filename, scans)
            queued_pass = find_utils.find_in_logs(DPX_PATH + "rawcook_dpx_success.log", file_scan_name)
            queued_fail = find_utils.find_in_logs(DPX_PATH + "tar_dpx_failures.log", file_scan_name)

            if not queued_pass and not queued_fail:
                logging_utils.log(self.logfile, "Metadata file creation has started for:")
                logging_utils.log(self.logfile, os.path.join(file_scan_name, dimensions, dpx))
                mediainfo_output_file = os.path.join(DPX_PATH, file_scan_name, f"{filename}_{dpx}_metadata.txt")
                tree_output_file = os.path.join(DPX_PATH, file_scan_name, f"{filename}_directory_contents.txt")
                size_output_file = os.path.join(DPX_PATH, file_scan_name, f"{filename}_directory_total_byte_size.txt")
                shell_utils.get_media_info('-f', os.path.join(dir_name, dpx), mediainfo_output_file)

                shell_utils.generate_tree(dir_name, '', tree_output_file)  # Check if we need this tree
                byte_size = os.path.getsize(DPX_PATH + filename)
                with open(size_output_file, 'a') as file:
                    file.write(
                        f"{filename} total folder size in bytes {str(byte_size)}")  # check if we need this wording
                # Start comparison of first dpx file against mediaconch policy
                check = shell_utils.check_mediaconch_policy(POLICY_PATH, os.path.join(dir_name, dpx))
                check_str = check.stdout.decode()

                if check_str.startswith("pass!"):
                    media_info = shell_utils.get_media_info('--Details=1', os.path.join(dir_name, dpx))
                    search_term = "Pixels per line:"
                    pixel_matches = [line for line in media_info.splitlines() if search_term.lower() in line.lower()]
                    pixels_per_line = int(re.split(r"\s+", pixel_matches[0])[4])
                    if pixels_per_line > 3999:
                        logging_utils.log(self.logfile,
                                          f"PASS: 4K scan {file_scan_name} has passed the MediaConch policy"
                                          f" and can progress to RAWcooked processing path")
                        with open(self.luma_4k_dpx_file, 'a') as file:
                            file.write(DPX_PATH + filename + "\n")
                    else:
                        search_term = "Descriptor"
                        descriptor_matches = [line for line in media_info.splitlines() if
                                              search_term.lower() in line.lower()]
                        luma_match = descriptor_matches[0].find("Luma (Y)")
                        if luma_match > 0:
                            logging_utils.log(self.logfile,
                                              f"PASS: Luma (Y) {file_scan_name} has passed the MediaConch policy"
                                              f" and can progress to RAWcooked processing path")
                            with open(self.luma_4k_dpx_file, 'a') as file:
                                file.write(DPX_PATH + filename + "\n")
                        else:
                            logging_utils.log(self.logfile,
                                              f"PASS: RGB {file_scan_name} has passed the MediaConch policy"
                                              f"  and can progress to RAWcooked processing path")
                            with open(self.rawcooked_dpx_file, 'a') as file:
                                file.write(DPX_PATH + filename + "\n")

                else:
                    logging_utils.log(self.logfile,
                                      f"FAIL: {file_scan_name} DOES NOT CONFORM TO MEDIACONCH POLICY. Adding to "
                                      f"tar_dpx_failures_list.txt")
                    logging_utils.log(self.logfile, check_str)
                    with open(self.tar_dpx_file, 'a') as file:
                        file.write(DPX_PATH + filename + "\n")


            else:
                logging_utils.log(self.logfile,
                                  "SKIPPING DPX folder, it has already been processed but has not moved to correct "
                                  "processing path:")
                logging_utils.log(self.logfile, file_scan_name)

    def move_passed_files(self):
        """Moves the processed and passed DPX sequences into the dpx_to_cook folder

        Collects all the folder (DPX sequence) names from the temp files
        (luma_4k_dpx_list.txt, rawcooked_dpx_list.txt and tar_dpx_list.txt).
        Then moves them into dpx_to_cook folder

        Once all the passed files are moved, the remaining are the files that failed mediaconch policies
        """
        dpx_folders_to_move = []
        txt_files_to_check = [self.rawcooked_dpx_file, self.luma_4k_dpx_file]
        for txt_file in txt_files_to_check:
            with open(txt_file, 'r') as file:
                sorted_paths = file.read().splitlines()
                dpx_folders_to_move.extend(sorted_paths)

        for file_path in dpx_folders_to_move:
            shutil.move(file_path, DPX_TO_COOK_PATH)

    def log_success_failure(self):
        # Append latest pass/failures to movement logs
        with open(self.success_file, 'a') as target:
            with open(self.rawcooked_dpx_file, 'r') as source:
                target.write(source.read())
            with open(self.luma_4k_dpx_file, 'r') as source:
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
        2. check_mediaconch_policy(depth): Check first .dpx file against mediaconch policies
        3. prepare_for_splitting(): Prepare files for splitting
        4. split(): Splits the files
        5. log_success_failure(): Logs the success or failure status
        6. clean(): Cleans up the temporary files
        """

        # TODO: Implement error handling mechanisms
        self.process()
        # TODO: Fix depth issue
        self.check_mediaconch_policy(3)  # Takes dept as argument. BROKEN

        self.move_passed_files()
        self.log_success_failure()
        self.clean()


if __name__ == '__main__':
    dpx_assessment = DpxAssessment()
    dpx_assessment.execute()
