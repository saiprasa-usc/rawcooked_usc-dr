import sys
from pathlib import Path
from dotenv import load_dotenv

import os
import re
import concurrent.futures
import shutil
from utils import find_utils, shell_utils, split_utils, logging_utils

# Load environment variables from .env file
load_dotenv()

SCRIPT_LOG = os.path.join(os.environ.get('FILM_OPS'), os.environ.get('DPX_SCRIPT_LOG'))
DPX_PATH = os.path.join(os.environ.get('FILM_OPS'), os.environ.get('DPX_ASSESS'))
PY3_LAUNCH = os.environ.get('PY3_ENV')
SPLITTING = os.environ.get('SPLITTING_SCRIPT_FILMOPS')
POLICY_PATH = os.path.join(os.environ.get('FILM_OPS'), os.environ.get('POLICY_DPX'))
RAWCOOKED_PATH = os.path.join(os.environ.get('FILM_OPS'), os.environ['DPX_COOK'])


class DpxAssessment:
    def __init__(self):
        self.logfile = os.path.join(SCRIPT_LOG, 'dpx_assessment.log')
        self.rawcooked_dpx_file = os.path.join(DPX_PATH, 'rawcooked_dpx_list.txt')
        self.luma_4k_dpx_file = os.path.join(DPX_PATH, 'luma_4k_dpx_list.txt')
        self.tar_dpx_file = os.path.join(DPX_PATH, 'tar_dpx_list.txt')
        self.python_file = os.path.join(DPX_PATH, 'python_list.txt')
        self.success_file = os.path.join(DPX_PATH, 'rawcook_dpx_success.log')
        self.failure_file = os.path.join(DPX_PATH, 'tar_dpx_failures.log')

        # Refresh temporary success/failure lists
        self.temp_files = [self.rawcooked_dpx_file, self.tar_dpx_file, self.luma_4k_dpx_file, self.python_file]

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

    def check_mediaconch_policy(self, depth=3) -> bool:
        """Checks if the files in each folder matches the mediaconch policies

        Loop that retrieves single DPX file in each folder, runs Mediaconch check and generates metadata files
        Configured for three level folders: N_123456_01of01/scan01/dimensions/<dpx_seq>
        depth should be 3 (Needs to be configurable. Work in progress)
        """

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
                with open(size_output_file, 'w') as file:
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
                        with open(self.luma_4k_dpx_file, 'w') as file:
                            file.write(DPX_PATH + filename)
                    else:
                        search_term = "Descriptor"
                        descriptor_matches = [line for line in media_info.splitlines() if
                                              search_term.lower() in line.lower()]
                        luma_match = descriptor_matches[0].find("Luma (Y)")
                        if luma_match > 0:
                            logging_utils.log(self.logfile,
                                              f"PASS: Luma (Y) {file_scan_name} has passed the MediaConch policy"
                                              f" and can progress to RAWcooked processing path")
                            with open(self.luma_4k_dpx_file, 'w') as file:
                                file.write(DPX_PATH + filename)
                        else:
                            logging_utils.log(self.logfile,
                                              f"PASS: RGB {file_scan_name} has passed the MediaConch policy"
                                              f"  and can progress to RAWcooked processing path")
                            with open(self.rawcooked_dpx_file, 'w') as file:
                                file.write(DPX_PATH + filename)

                else:
                    logging_utils.log(self.logfile,
                                      f"FAIL: {file_scan_name} DOES NOT CONFORM TO MEDIACONCH POLICY. Adding to "
                                      f"tar_dpx_failures_list.txt")
                    logging_utils.log(self.logfile, check_str)
                    with open(self.tar_dpx_file, 'w') as file:
                        file.write(DPX_PATH + filename)

                    return False

            else:
                logging_utils.log(self.logfile,
                                  "SKIPPING DPX folder, it has already been processed but has not moved to correct "
                                  "processing path:")
                logging_utils.log(self.logfile, file_scan_name)
        return True

    def prepare_for_splitting(self):
        # Prepare luma_4k_dpx_list for DPX splitting script/move to RAWcooked preservation
        luma_4k_list = split_utils.sort_split_list(self.luma_4k_dpx_file)
        if luma_4k_list:
            logging_utils.log(self.logfile,
                              "RAWcooked Luma Y/4K path items for size check and Python splitting/moving script:")
            logging_utils.log(self.logfile, "\n".join(luma_4k_list))
            luma_4k_size_dict = split_utils.create_python_list(self.luma_4k_dpx_file, self.python_file, "luma_4k")
            for key in luma_4k_size_dict:
                logging_utils.log(self.logfile,
                                  f"Size of {key} is {luma_4k_size_dict[key]} KB. Passing to Python script...")

        # Prepare tar_dpx_failure_list for DPX splitting script/move to TAR preservation
        tar_dpx_failure_list = split_utils.sort_split_list(self.tar_dpx_file)
        if tar_dpx_failure_list:
            logging_utils.log(self.logfile, "TAR path items for size check and Python splitting/moving script:")
            logging_utils.log(self.logfile, "\n".join(tar_dpx_failure_list))
            tar_size_dict = split_utils.create_python_list(self.tar_dpx_file, self.python_file, "tar")
            for key in tar_size_dict:
                logging_utils.log(self.logfile,
                                  f"Size of {key} is {tar_size_dict[key]} KB. Passing to Python script...")

        # Prepare dpx_success_list for DPX splitting script/move to TAR preservation
        dpx_success_list = split_utils.sort_split_list(self.rawcooked_dpx_file)
        if dpx_success_list:
            logging_utils.log(self.logfile,
                              "RAWcooked 2K RGB path items for size check and Python splitting/moving script:")
            logging_utils.log(self.logfile, "\n".join(dpx_success_list))
            rawcooked_size_dict = split_utils.create_python_list(self.rawcooked_dpx_file, self.python_file, "rawcooked")
            for key in rawcooked_size_dict:
                logging_utils.log(self.logfile,
                                  f"Size of {key} is {rawcooked_size_dict[key]} KB. Passing to Python script...")

    def split(self):
        if os.path.getsize(self.python_file) > 0:
            logging_utils.log(self.logfile,
                              "Launching python script to process DPX sequences. Please see dpx_splitting_script.log "
                              "for more details. IGNORE LOG NO SPLITTING OCCURS")

            with open(self.python_file, 'r') as file:
                file_list = set(file.read().splitlines())
                dpx_path = [file.split('%')[1].strip() for file in file_list]
            for file in dpx_path:
                shutil.move(file, RAWCOOKED_PATH)
            # with concurrent.futures.ThreadPoolExecutor() as executor:
            # Change to parallel processes rather than threads
            #     futures = [executor.submit(run_script, PY3_LAUNCH, SPLITTING, argument) for argument in file_list]
            #     concurrent.futures.wait(futures)

            logging_utils.log(self.logfile,
                              "===================== DPX assessment workflows ENDED =====================")

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

        # TODO: The DPX folders are not deleted from the dpx_to_assess. As it was happening
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
        passed = self.check_mediaconch_policy(3)  # Takes dept as argument. BROKEN
        if passed:
            # TODO: Completely remove splitting scripts and their dependencies
            self.prepare_for_splitting()
            self.split()
        self.log_success_failure()
        self.clean()


if __name__ == '__main__':
    dpx_assessment = DpxAssessment()
    dpx_assessment.execute()
