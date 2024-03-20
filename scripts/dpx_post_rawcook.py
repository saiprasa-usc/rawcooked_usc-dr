import shutil
import mmap
import sys
from datetime import datetime

from utils.logging_utils import log

from dotenv import load_dotenv

import os

from utils.shell_utils import check_mediaconch_policy

load_dotenv()

# ====================================================================
# === Clean up and inspect logs for problem DPX sequence encodings ===
# ====================================================================
ERRORS = os.path.join(os.environ.get('FILM_OPS'), os.environ.get('CURRENT_ERRORS'))
DPX_PATH = os.path.join(os.environ.get('FILM_OPS'), os.environ.get('RAWCOOKED_PATH'))
DPX_DEST = os.path.join(os.environ.get('FILM_OPS'), os.environ.get('DPX_COMPLETE'))
MKV_DESTINATION = os.path.join(os.environ.get('FILM_OPS'), os.environ.get('MKV_ENCODED'))
CHECK_FOLDER = os.path.join(os.environ.get('FILM_OPS'), os.environ.get('MKV_CHECK'))
MKV_POLICY = os.path.join(os.environ.get('FILM_OPS'), os.environ.get('POLICY_RAWCOOK'))
SCRIPT_LOG = os.path.join(os.environ.get('FILM_OPS'), os.environ.get('DPX_SCRIPT_LOG'))
REVIEW_FAILS_PATH = os.path.join(os.environ.get('FILM_OPS'), os.environ.get('DPX_REVIEW'), 'post_rawcook_fails/')
MKV_COOKED_PATH = os.path.join(MKV_DESTINATION, 'mkv_cooked/')


class DpxPostRawcook:
    def __init__(self):
        self.logfile = os.path.join(SCRIPT_LOG, "dpx_post_rawcook.log")
        self.date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.temp_medicaconch_policy_fails_file = os.path.join(MKV_DESTINATION, "temp_medicaconch_policy_fails.txt")

    def process(self):
        """Initiates the Post Rawcooked Workflow

        Creates the temporary and log files (if not present)
        Also checks if the mkv_cooked folder have any files to process
        """
        if not os.path.exists(self.logfile):
            with open(self.logfile, 'w+'):
                pass

        # Check mkv_cooked/ folder populated before starting log writes
        if any(os.scandir(MKV_DESTINATION + 'mkv_cooked/')):
            log(self.logfile, "===================== Post-RAWcook workflows STARTED =====================")
            log(self.logfile, "Files present in mkv_cooked folder, checking if ready for processing...")
        else:
            print("MKV folder empty, script exiting")
            sys.exit(1)

        with open(self.temp_medicaconch_policy_fails_file, 'w+'):
            pass

    def check_mediaconch_policies(self):
        """For every .mkv files it checks against a policy using mediaconch

        Scans through the mkv_cooked folder for .mkv files and run mediaconch
        If a file fails the check, the path of that file and the respective .mkv.txt file gets appended into a
        temporary .txt file named temp_medicaconch_policy_fails.txt
        """
        with os.scandir(MKV_COOKED_PATH) as entries:
            for entry in entries:
                file_name = entry.name
                if file_name.endswith(".mkv"):
                    file_path = entry.path
                    mediaconch_output = check_mediaconch_policy(MKV_POLICY, file_path)
                    standard_output = mediaconch_output.stdout.decode()
                    if "pass!" in standard_output:  # check_str.startswith('pass!'):
                        log(self.logfile,
                            f"PASS: RAWcooked MKV file {file_name} has passed the Mediaconch policy")
                    else:
                        log(self.logfile, f"FAIL: RAWcooked MKV {file_name} has failed the mediaconch policy")
                        log(self.logfile, f"MEDIACONCH_FAILED_RESULT: {mediaconch_output}")

                        # Write both the failed mkv file along with the corresponding txt file
                        with open(self.temp_medicaconch_policy_fails_file, "a+") as file:
                            txt_file_name = f"{file_name}.txt"
                            txt_file_path = os.path.join(MKV_COOKED_PATH, txt_file_name)
                            file.write(f"{file_path}\n")
                            file.write(f"{txt_file_path}\n")

    def move_failed_files(self):
        """Moves the failed mkv file and its corresponding txt file into dpx_for_review/post_rawcook_fails/

        Reads temp_medicaconch_policy_fails.txt
        Moves the .mkv files into dpx_for_review/post_rawcook_fails/mkv_files/
        Moves the .mkv.txt files into  dpx_for_review/post_rawcook_fails/rawcook_output_logs/
        Remaining .mkv files inside the mkv_cooked have passed the mediaconch checks
        """
        with open(self.temp_medicaconch_policy_fails_file, 'r') as file:
            for file_path in file:
                file_path = file_path.strip()
                if file_path.endswith(".mkv"):
                    print(file_path)
                    shutil.move(file_path, os.path.join(REVIEW_FAILS_PATH, 'mkv_files/'))
                elif file_path.endswith(".txt"):
                    shutil.move(file_path, os.path.join(REVIEW_FAILS_PATH, 'rawcook_output_logs/'))

    def check_general_errors(self):
        """Checks the mediaconch passed .mkv files for any other RAWCooked errors

        Reads each of the .mkv.txt files and checks for messages stored in error_messages
        If error found, then the repective .mkv file are moved to dpx_for_review/post_rawcook_fails/mkv_files/
        And the .mkv.txt files are moved to dpx_for_review/post_rawcook_fails/rawcook_output_logs/
        """
        error_messages = [b"Reversibility was checked, issues detected, see below.", b"Error:", b"Conversion failed!",
                          b"Please contact info@mediaarea.net if you want support of such content."]

        error_file_path_list = []
        with os.scandir(MKV_COOKED_PATH) as files:
            txt_file_list = [file.path for file in files if file.name.endswith("mkv.txt")]

        # used mmap as byte operations are faster
        for txt_file_path in txt_file_list:
            with open(txt_file_path, 'rb', 0) as file:
                s = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
                for msg in error_messages:
                    if s.find(msg) != -1:
                        error_file_path_list.append(txt_file_path)
                        break

        # Moves files with general error into dpx_for_review/post_rawcook_fails
        for txt_file_path in error_file_path_list:
            txt_file_name = txt_file_path.split('/')[-1]
            mkv_file_name = txt_file_name.replace('.txt', '')
            mkv_file_path = txt_file_path.replace('.txt', '')

            if os.path.exists(mkv_file_path):
                log(self.logfile, f"UNKNOWN ENCODING ERROR: {mkv_file_name} encountered error")
                log(self.logfile, f"Moving {mkv_file_name} for manual review")
                shutil.move(mkv_file_path, os.path.join(REVIEW_FAILS_PATH, 'mkv_files/'))
            else:
                log(self.logfile, f"UNKNOWN ENCODING ERROR: {mkv_file_name} was not created by RawCooked. "
                                  f"Check {txt_file_name} for Rawcooked logs")

            log(self.logfile, f"Moving {txt_file_name} for manual review")
            shutil.move(txt_file_path, os.path.join(REVIEW_FAILS_PATH, 'rawcook_output_logs/'))

    def clean(self):
        """Concludes the workflow

        Deletes all the temporary .txt files created during the workflow
        """
        log(self.logfile, f"Concluding workflow by deleting temporary files")
        os.remove(self.temp_medicaconch_policy_fails_file)
        log(self.logfile, f"============= DPX Post-RAWcook workflow ENDED =============")

    def execute(self):
        self.process()
        self.check_mediaconch_policies()
        self.move_failed_files()
        self.check_general_errors()
        self.clean()


if __name__ == '__main__':
    post_rawcook = DpxPostRawcook()
    post_rawcook.execute()
