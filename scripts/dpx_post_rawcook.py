import shutil

from datetime import datetime

from utils.logging_utils import log

from dotenv import load_dotenv

import os

from utils.shell_utils import check_mediaconch_policy, move_files_parallel

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

logfile = os.path.join(SCRIPT_LOG, "dpx_post_rawcook.log")
date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


class DpxPostRawcook:
    def __init__(self):
        self.temp_medicaconch_policy_fails_file = os.path.join(MKV_DESTINATION, "temp_medicaconch_policy_fails.txt")
        self.successfull_mkv_list_file = os.path.join(MKV_DESTINATION, "successfull_mkv_list.txt")
        self.matroska_deletion_file = os.path.join(MKV_DESTINATION, "matroska_deletion.txt")
        self.matroske_deletion_list_file = os.path.join(MKV_DESTINATION, "matroske_deletion_list.txt")
        self.stale_encodings_file = os.path.join(MKV_DESTINATION, "stale_encodings.txt")
        self.error_list_file = os.path.join(MKV_DESTINATION, "error_list.txt")
        self.reversibility_list_file = os.path.join(MKV_DESTINATION, "reversibility_list.txt")

        self.file_names = [
            self.temp_medicaconch_policy_fails_file, self.successfull_mkv_list_file, self.matroska_deletion_file,
            self.matroske_deletion_list_file, self.stale_encodings_file, self.error_list_file,
            self.reversibility_list_file
        ]

    def process(self):
        # Check mkv_cooked/ folder populated before starting log writes
        if any(os.scandir(MKV_DESTINATION + 'mkv_cooked/')):
            log(logfile, "===================== Post-RAWcook workflows STARTED =====================")
            log(logfile, "Files present in mkv_cooked folder, checking if ready for processing...")
        else:
            print("MKV folder empty, script exiting")

        for file_name in self.file_names:
            with open(file_name, 'w'):
                pass

    def check_mediaconch_policies(self):
        # ==========================================================================
        # Matroska checks using MediaConch policy, remove fails to Killed folder ===
        # ==========================================================================
        with os.scandir(MKV_COOKED_PATH) as entries:
            for entry in entries:
                file_name = entry.name

                if file_name.endswith(".mkv"):
                    file_path = os.path.join(MKV_COOKED_PATH, file_name)
                    mediaconch_output = check_mediaconch_policy(MKV_POLICY, file_path)
                    standard_output = mediaconch_output.stdout.decode()
                    if "pass!" in standard_output:  # check_str.startswith('pass!'):
                        log(logfile,
                            f"PASS: RAWcooked MKV file {file_name} has passed the Mediaconch policy")
                    else:
                        log(logfile, f"FAIL: RAWcooked MKV {file_name} has failed the mediaconch policy")
                        log(logfile, f"MEDIACONCH_FAILED_RESULT: {mediaconch_output}")

                        # Write both the failed mkv file along with the corresponding txt file
                        with open(self.temp_medicaconch_policy_fails_file, "a+") as file:
                            txt_file_name = f"{file_name}.txt"
                            txt_file_path = os.path.join(MKV_COOKED_PATH, txt_file_name)
                            file.write(f"{file_path}\n")
                            file.write(f"{txt_file_path}\n")

    # Moves the failed mkv file and its corresponding txt file into dpx_for_review/post_rawcook_fails
    def move_failed_files(self):
        with open(self.temp_medicaconch_policy_fails_file, 'r') as file:
            for file_path in file:
                file_path = file_path.strip()
                if file_path.endswith(".mkv"):
                    print(file_path)
                    shutil.move(file_path, os.path.join(REVIEW_FAILS_PATH, 'mkv_files/'))
                elif file_path.endswith(".txt"):
                    shutil.move(file_path, os.path.join(REVIEW_FAILS_PATH, 'rawcook_output_logs/'))

    def move_success_files(self):
        # ===================================================================================
        # Log check passes move to MKV Check folder and logs folders, and DPX folder move ===
        # ===================================================================================
        with os.scandir(MKV_DESTINATION + "mkv_cooked/") as entries:
            for entry in entries:
                filename = entry.name
                if filename.endswith("mkv.txt"):
                    with open(MKV_DESTINATION + "mkv_cooked/" + filename, 'r') as file:
                        for line in file:
                            if line.find('Reversibility was checked, no issue detected.') != -1:
                                mkv_filename = filename.split('.')[0] + '.mkv'
                                dpx_success_path = entry.path
                                log(logfile,
                                    "COMPLETED: RAWcooked MKV " + mkv_filename + "has completed successfully and will "
                                                                                 "be moved to check folder")
                                with open(MKV_DESTINATION + "rawcooked_success.log", "a") as f:
                                    f.write(dpx_success_path)
                                with open(self.successfull_mkv_list_file, "a") as f:
                                    f.write(mkv_filename)
                            else:
                                log(logfile,
                                    "SKIP: Matroska " + mkv_filename + " has not completed, or has errors detected")

        # Move successfully encoded MKV files to check folder
        successful_mkv_list = []
        successful_mkv_txt_list = []
        with open(self.successfull_mkv_list_file, 'r') as file:
            for line in file:
                if line.endswith(".mkv"):
                    successful_mkv_list.append(line)
                elif line.endswith(".txt"):
                    successful_mkv_txt_list.append(line)

        # Move successfully encoded MKV files to check folder
        move_files_parallel(os.path.join(MKV_DESTINATION, "mkv_cooked/"), CHECK_FOLDER, successful_mkv_list, 10)

        # Move the successful txt files to logs folder
        move_files_parallel(os.path.join(MKV_DESTINATION, "mkv_cooked/"), os.path.join(MKV_DESTINATION, 'logs/'),
                            successful_mkv_txt_list, 10)

        # Move successful DPX sequence folders to dpx_completed/
        successful_mkv_folder_list = [item.split('.')[0] for item in successful_mkv_list]

        # Add list of moved items to post_rawcooked.log
        if os.path.getsize(self.successfull_mkv_list_file) > 0:
            # Log the message
            log(logfile,
                "Successful Matroska files moved to check folder, DPX sequences for each moved to dpx_completed:\n")
            with open(logfile, 'w') as file:
                file.write("\n".join(os.listdir(self.successfull_mkv_list_file)))
        else:
            print("File is empty, skipping")

    def check_big_reversibility_file(self):
        # ==========================================================================
        # Error: the reversibility file is becoming big. --output-version 2 pass ===
        # =========================================================================
        with os.scandir(os.path.join(MKV_DESTINATION, "mkv_cooked/")) as entries:
            for entry in entries:
                if entry.name.endswith(".mkv.txt"):
                    error_check = False
                    retry_check = False
                    mkv_fname1 = entry.name
                    dpx_folder1 = mkv_fname1.split(".")[0]
                    with open(f"{MKV_DESTINATION}mkv_cooked/{entry.name}") as file:
                        for line in file:
                            if line.find(
                                    "'Error: undecodable file is becoming too big.\|Error: the reversibility file is "
                                    "becoming big.'") != -1:
                                error_check = True
                    if error_check:
                        log(logfile, "MKV " + mkv_fname1 + " log has no large reversibility file warning. Skipping")
                    else:
                        with open(self.reversibility_list_file) as file:
                            for line in file:
                                if line.find(mkv_fname1):
                                    retry_check = True
                        file.close()
                        if retry_check:
                            log(logfile, f"NEW ENCODING ERROR: ${mkv_fname1} adding to reversibility_list")
                            with open(self.reversibility_list_file, "w") as file:
                                file.write(f"{DPX_PATH}dpx_to_cook/{dpx_folder1}")
                            file.close()
                            shutil.move(entry.name, MKV_DESTINATION + "logs/retry_" + mkv_fname1 + ".txt")

                        else:
                            log(logfile,
                                f"REPEAT ENCODING ERROR: {mkv_fname1} encountered repeated reversilibity data error")
                            with open(ERRORS + dpx_folder1 + "_errors.log", "w+") as file:
                                file.write(
                                    f"post_rawcooked {date}: {mkv_fname1} Repeated reversibility data error for "
                                    f"sequence:")
                                file.write(f"    {DPX_PATH}dpx_to_cook/{dpx_folder1}")
                                file.write("    The FFV1 Matroska will be deleted.")
                                file.write(
                                    "Please contact the Knowledge and Collections Developer about this repeated "
                                    "reversibility failure.")
                            file.close()
                            with open(self.matroska_deletion_file, "w") as file:
                                file.write(mkv_fname1)
                            file.close()
                            shutil.move(MKV_DESTINATION + "mkv_cooked/" + entry.name,
                                        MKV_DESTINATION + "logs/fail_" + mkv_fname1 + ".txt")

        # Add list of reversibility data error to dpx_post_rawcooked.log
        if os.path.getsize(self.matroska_deletion_file) > 0:
            log(logfile, "MKV files that will be deleted due to reversibility data error in logs (if present):")
            with open(self.matroska_deletion_file, 'r') as input_file, open(logfile, 'a') as output_file:
                deletion_file_name = input_file.read()
                output_file.write(deletion_file_name)
                # Delete broken Matroska files if they exist (unlikely as error exits before encoding)
                if os.path.exists(MKV_DESTINATION + "mkv_cooked/" + deletion_file_name):
                    os.remove(MKV_DESTINATION + "mkv_cooked/" + deletion_file_name)
            input_file.close()
            output_file.close()
        else:
            print("No MKV files for deletion. Skipping.")

        # Add reversibility list to logs for reference
        if os.path.getsize(self.reversibility_list_file):
            log(logfile, "DPX sequences that will be re-encoded using --output-version 2:")
            with open(self.reversibility_list_file, 'r') as input_file, open(logfile, 'a') as output_file:
                output_file.write(input_file.read())
            input_file.close()
            output_file.close()
        else:
            print("No DPX sequences for re-encoding using --output-version 2. Skipping.")

        # TODO make these checks functions rather than checking everything separately

    def check_general_errors(self):

        # ===================================================================================
        # General Error/Warning message failure checks - retry or raise in current errors ===
        # ===================================================================================
        error_messages = ["Reversibility was checked, issues detected, see below.", "Error:", "Conversion failed!",
                          "Please contact info@mediaarea.net if you want support of such content."]
        with os.scandir(os.path.join(MKV_DESTINATION, "mkv_cooked/")) as entries:
            for entry in entries:
                error_check = False
                mkv_fname = entry.name
                dpx_folder = entry.path
                if entry.name.endswith("mkv.txt"):
                    with open(entry.name, "r") as file:
                        for line in file:
                            for message in error_messages:
                                if message in line:
                                    error_check = True
                if error_check:
                    log(logfile, f"UNKNOWN ENCODING ERROR: {mkv_fname} encountered error")
                    with open(dpx_folder + "_errors.log", "w") as file:
                        file.write(DPX_PATH + "dpx_to_cook/" + dpx_folder)
                        file.write(f"post_rawcooked {date}: {mkv_fname} Repeat encoding error raised for sequence:")
                        file.write(f"    {DPX_PATH}dpx_to_cook/{dpx_folder}")
                        file.write(f"    Matroska file will be deleted.")
                        file.write(
                            f"    Please contact the Knowledge and Collections Developer about this repeated encoding "
                            f"failure")

                    with open(self.matroska_deletion_file, "a") as file:
                        file.write(f"{mkv_fname}")
                    file.close()
                    shutil.move(entry.name, MKV_DESTINATION + "logs/fail_" + mkv_fname + ".txt")
                else:
                    log(logfile,
                        f"MKV {mkv_fname} log has no error messages. Likely an interrupted or incomplete encoding")

    def killed_process_workflow(self):
        # ===============================================================
        # FOR ==== INCOMPLETE ==== - i.e. killed processes ==============
        # ===============================================================

        # This block manages the remaining INCOMPLETE cooks that have been killed or stalled mid-encoding
        with os.scandir(MKV_DESTINATION + "mkv_cooked/") as entries:
            for entry in entries:
                if entry.name.endswith(".mkv.txt"):
                    stale_fname = entry.name
                    stale_basename = entry.path
                    fname_log = entry.path.split('/')[0]
                    log(logfile,
                        f"Stalled/killed encoding: {stale_basename}. Adding to stalled list and deleting log file and "
                        f"Matroska")
                    with open(self.stale_encodings_file, "w") as file:
                        file.write(stale_fname)
                    with open(ERRORS + fname_log + "_errors.log", "a") as file:
                        file.write(f"post_rawcooked {date}: Matroka {stale_fname} encoding stalled mid_process.")
                        file.write(f"    Matrosk and failed log deleted. DPX sequence will retry RAWCooked encoding.")
                        file.write(
                            f" Please contact the Knowledge and Collections Developer if this item repeatedly stalls.")

        # Add list of stalled logs to post_rawcooked.log
        if os.path.getsize(self.stale_encodings_file) > 0:
            log(logfile, "Stalled files that will be deleted:")
            with open(self.stale_encodings_file, 'r') as input_file, open(logfile, 'a') as output_file:
                deletion_file_name = input_file.read()
                output_file.write(deletion_file_name)
                # Delete broken log files and delete broken Matroska files if they exist
                os.remove(deletion_file_name)
            input_file.close()
            output_file.close()
        else:
            print("No stale encodings to process at this time. Skipping.")

        # Write an END note to the logfile
        log(logfile, "===================== Post-rawcook workflows ENDED =====================")

    def update_success_count(self):
        # Update the count of successful cooks at top of the success log
        # First create new temp_success_log with timestamp
        with open(MKV_DESTINATION + "temp_rawcooked_success.log", "w") as output_file:
            output_file.write(f"===================== Updated ===================== {date}")
            # Count lines in success_log and create count variable, output that count to new success log, then output
            # all lines with /home* to the new log
            with open(MKV_DESTINATION + "rawcooked_success.log", "r") as input_file:
                lines = input_file.readlines()
                success_count = len(lines)
                for line in lines:
                    output_file.write(line)
            output_file.write(f"===================== Successful cooks: {success_count} ===================== {date}")

    def clean(self):
        # Sort the log and remove any non-unique lines
        with open(MKV_DESTINATION + "temp_rawcooked_success.log", "r") as file:
            lines = file.readlines()
            unique = list(set(lines))
            unique.sort()
        file.close()

        with open(MKV_DESTINATION + "temp_rawcooked_success_unique.log", "w") as file:
            file.writelines(unique)

        for filename in self.file_names:
            os.remove(filename)

        shutil.move(MKV_DESTINATION + "temp_rawcooked_success_unique.log", MKV_DESTINATION + "rawcooked_success.log")

    def execute(self):
        self.process()
        self.check_mediaconch_policies()
        # self.move_success_files()
        # self.check_big_reversibility_file()
        # self.check_general_errors()
        # self.killed_process_workflow()
        # self.update_success_count()
        self.clean()


if __name__ == '__main__':
    post_rawcook = DpxPostRawcook()
    post_rawcook.execute()
