import shutil
import subprocess
from datetime import datetime

from utils.logging_utils import log

from dotenv import load_dotenv

import os

from utils.shell_utils import check_mediaconch_policy, move_files_parallel

load_dotenv()

# ====================================================================
# === Clean up and inspect logs for problem DPX sequence encodings ===
# ====================================================================
ERRORS = os.environ.get('FILM_OPS') + os.environ.get('CURRENT_ERRORS')
DPX_PATH = os.environ.get('FILM_OPS') + os.environ.get('RAWCOOKED_PATH')
DPX_DEST = os.environ.get('FILM_OPS') + os.environ.get('DPX_COMPLETE')
MKV_DESTINATION = os.environ.get('FILM_OPS') + os.environ.get('MKV_ENCODED')
CHECK_FOLDER = os.environ.get('FILM_OPS') + os.environ.get('MKV_CHECK')
MKV_POLICY = os.environ.get('POLICY_RAWCOOK')
SCRIPT_LOG = os.environ.get('FILM_OPS') + os.environ.get('DPX_SCRIPT_LOG')

logfile = SCRIPT_LOG + "dpx_post_rawcook.log"
date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Check mkv_cooked/ folder populated before starting log writes
if any(os.scandir(MKV_DESTINATION + 'mkv_cooked/')):
    log(logfile, "===================== Post-RAWcook workflows STARTED =====================")
    log(logfile, "Files present in mkv_cooked folder, checking if ready for processing...")
else:
    print("MKV folder empty, script exiting")

# Script temporary file recreate (delete at end of script)
temp_medicaconch_policy_fails_file = MKV_DESTINATION + "temp_medicaconch_policy_fails.txt"
successfull_mkv_list_file = MKV_DESTINATION + "successfull_mkv_list.txt"
matroska_deletion_file = MKV_DESTINATION + "matroska_deletion.txt"
matroske_deletion_list_file = MKV_DESTINATION + "matroske_deletion_list.txt"
stale_encodings_file = MKV_DESTINATION + "stale_encodings.txt"
error_list_file = MKV_DESTINATION + "error_list.txt"
reversibility_list_file = MKV_DESTINATION + "reversibility_list.txt"

file_names = [temp_medicaconch_policy_fails_file, successfull_mkv_list_file, matroska_deletion_file,
              matroske_deletion_list_file, stale_encodings_file, error_list_file, reversibility_list_file]

for file_name in file_names:
    with open(file_name, 'w') as f:
        pass

# =======================================================================================
# Matroska size check remove files to Killed folder, and folders moved to check_size/ ===
# DO NOT NEED THIS LOGIC --- ******************SKIPPING**************************** -----
# =======================================================================================

# ==========================================================================
# Matroska checks using MediaConch policy, remove fails to Killed folder ===
# ==========================================================================
with os.scandir(MKV_DESTINATION + "mkv_cooked/") as entries:
    for entry in entries:
        filename = entry.name
        fname_log = filename.split('.')[0]
        if filename.endswith(".mkv") or filename.endswith(
                ".rawcooked_reversibility_data"):  # TODO check this file name ending. Should be .mkv only
            check = check_mediaconch_policy(MKV_POLICY, filename)
            check_str = check.stdout.decode()
            if ".mkv.r" in check_str:  # check_str.startswith('pass!'):
                log(logfile, "PASS: RAWcooked MKV file " + filename + " has passed the Mediaconch policy. Whoopee")
            else:
                log(logfile, "FAIL: RAWcooked MKV " + filename + " has failed the mediaconch policy")
                log(logfile, "Moving " + filename + " to killed directory, and amending log fail_" + filename + ".txt")
                log(logfile, check)
                with open(ERRORS + fname_log + "_errors.log", 'a+') as file:
                    file.write(
                        "post_rawcooked" + date + "): Matroska " + filename + " failed the FFV1 MKV Mediaconch policy.")
                    file.write("    Matroska moved to Killed folder, DPX sequence will retry RAWcooked encoding.")
                    file.write(
                        "    Please contact the Knowledge and Collections Developer about this Mediaconch policy failure.")
                with open(temp_medicaconch_policy_fails_file, "a+") as file:
                    file.write(filename)

# Move failed MKV files to killed folder
failed_mkv_file_list = []
failed_txt_file_list = []
with open(temp_medicaconch_policy_fails_file, 'r') as file:
    for line in file:
        if line.endswith(".mkv"):
            failed_mkv_file_list.append(line)
        elif line.endswith(".txt"):
            failed_txt_file_list.append(line)
move_files_parallel(MKV_DESTINATION + 'mkv_cooked/', MKV_DESTINATION + 'killed/', failed_mkv_file_list,
                    10)  # TODO rename mkv_cooked to a vairable
# Move the txt files to logs folder and prepend -fail- to filename
move_files_parallel(MKV_DESTINATION + 'mkv_cooked/', MKV_DESTINATION + 'logs/', failed_txt_file_list,
                    10)  # TODO prepend fail_

# ===================================================================================
# Log check passes move to MKV Check folder and logs folders, and DPX folder move ===
# ===================================================================================
with os.scandir(MKV_DESTINATION + "mkv_cooked/") as entries:
    for entry in entries:
        filename = entry.name
        if filename.endswith("mkv.txt"):
            with open(MKV_DESTINATION + "mkv_cooked/" + filename, 'r') as file:
                for line in file:
                    if (line.find('Reversibility was checked, no issue detected.') != -1):
                        mkv_filename = filename.split('.')[0] + '.mkv'
                        dpx_success_path = entry.path
                        log(logfile,
                            "COMPLETED: RAWcooked MKV " + mkv_filename + "has completed successfully and will be moved to check folder")
                        with open(MKV_DESTINATION + "rawcooked_success.log", "a") as file:
                            file.write(dpx_success_path)
                        with open(successfull_mkv_list_file, "a") as file:
                            file.write(mkv_filename)
                    else:
                        log(logfile, "SKIP: Matroska " + mkv_filename + " has not completed, or has errors detected")

# Move successfully encoded MKV files to check folder
successful_mkv_list = []
successful_mkv_txt_list = []
with open(successfull_mkv_list_file, 'r') as file:
    for line in file:
        if line.endswith(".mkv"):
            successful_mkv_list.append(line)
        elif line.endswith(".txt"):
            successful_mkv_txt_list.append(line)

# Move successfully encoded MKV files to check folder
move_files_parallel(MKV_DESTINATION + "mkv_cooked/", CHECK_FOLDER, successful_mkv_list, 10)

# Move the successful txt files to logs folder
move_files_parallel(MKV_DESTINATION + "mkv_cooked/", MKV_DESTINATION + 'logs/', successful_mkv_txt_list, 10)

# Move successful DPX sequence folders to dpx_completed/
successful_mkv_folder_list = [item.split('.')[0] for item in successful_mkv_list]

# Add list of moved items to post_rawcooked.log
if os.path.getsize(successfull_mkv_list_file) > 0:
    # Log the message
    log(logfile, "Successful Matroska files moved to check folder, DPX sequences for each moved to dpx_completed:\n")
    with open(logfile, 'w') as file:
        file.write("\n".join(os.listdir(successfull_mkv_list_file)))
else:
    print("File is empty, skipping")

# ==========================================================================
# Error: the reversibility file is becoming big. --output-version 2 pass ===
# =========================================================================
with os.scandir(MKV_DESTINATION + "mkv_cooked/") as entries:
    for entry in entries:
        if entry.name.endswith(".mkv.txt"):
            error_check = False
            retry_check = False
            mkv_fname1 = entry.name
            dpx_folder1 = mkv_fname1.split(".")[0]
            with open(MKV_DESTINATION + "mkv_cooked/" + entry.name) as file:
                for line in file:
                    if line.find(
                            "'Error: undecodable file is becoming too big.\|Error: the reversibility file is becoming big.'") != -1:
                        error_check = True
            file.close()  # TODO close all opened files
            if error_check:
                log(logfile, "MKV " + mkv_fname1 + " log has no large reversibility file warning. Skipping")
            else:
                with open(reversibility_list_file) as file:
                    for line in file:
                        if line.find(mkv_fname1):
                            retry_check = True
                file.close()
                if retry_check:
                    log(logfile, f"NEW ENCODING ERROR: ${mkv_fname1} adding to reversibility_list")
                    with open(reversibility_list_file, "w") as file:
                        file.write(f"{DPX_PATH}dpx_to_cook/{dpx_folder1}")
                    file.close()
                    shutil.move(entry.name, MKV_DESTINATION + "logs/retry_" + mkv_fname1 + ".txt")

                else:
                    log(logfile, f"REPEAT ENCODING ERROR: {mkv_fname1} encountered repeated reversilibity data error")
                    with open(ERRORS + dpx_folder1 + "_errors.log", "w+") as file:
                        file.write(
                            f"post_rawcooked {date}: ${mkv_fname1} Repeated reversibility data error for sequence:")
                        file.write(f"    {DPX_PATH}dpx_to_cook/${dpx_folder1}")
                        file.write("    The FFV1 Matroska will be deleted.")
                        file.write(
                            "    Please contact the Knowledge and Collections Developer about this repeated reversibility failure.")
                    file.close()
                    with open(matroska_deletion_file, "w") as file:
                        file.write(mkv_fname1)
                    file.close()
                    shutil.move(MKV_DESTINATION + "mkv_cooked/" + entry.name,
                                MKV_DESTINATION + "logs/fail_" + mkv_fname1 + ".txt")

# Add list of reversibility data error to dpx_post_rawcooked.log
if os.path.getsize(matroska_deletion_file) > 0:
    log(logfile, "MKV files that will be deleted due to reversibility data error in logs (if present):")
    with open(matroska_deletion_file, 'r') as input_file, open(logfile, 'a') as output_file:
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
if os.path.getsize(reversibility_list_file):
    log(logfile, "DPX sequences that will be re-encoded using --output-version 2:")
    with open(reversibility_list_file, 'r') as input_file, open(logfile, 'a') as output_file:
        output_file.write(input_file.read())
    input_file.close()
    output_file.close()
else:
    print("No DPX sequences for re-encoding using --output-version 2. Skipping.")

# TODO make these checks functions rather than checking everything separately

# ===================================================================================
# General Error/Warning message failure checks - retry or raise in current errors ===
# ===================================================================================
error_messages = ["Reversibility was checked, issues detected, see below.", "Error:", "Conversion failed!",
                  "Please contact info@mediaarea.net if you want support of such content."]
with os.scandir(MKV_DESTINATION + "mkv_cooked/") as entries:
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
            file.close()
        if error_check:
            log(logfile, f"UNKNOWN ENCODING ERROR: {mkv_fname} encountered error")
            with open(dpx_folder + "_errors.log", "w") as file:
                file.write(DPX_PATH + "dpx_to_cook/" + dpx_folder)
                file.write(f"post_rawcooked {date}: {mkv_fname} Repeat encoding error raised for sequence:")
                file.write(f"    {DPX_PATH}dpx_to_cook/{dpx_folder}")
                file.write(f"    Matroska file will be deleted.")
                file.write(
                    f"    Please contact the Knowledge and Collections Developer about this repeated encoding failure")
            file.close()
            with open(matroska_deletion_file, "a") as file:
                file.write(f"{mkv_fname}")
            file.close()
            shutil.move(entry.name, MKV_DESTINATION + "logs/fail_" + mkv_fname + ".txt")
        else:
            log(logfile, f"MKV {mkv_fname} log has no error messages. Likely an interrupted or incomplete encoding")

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
                f"Stalled/killed encoding: {stale_basename}. Adding to stalled list and deleting log file and Matroska")
            with open(stale_encodings_file, "w") as file:
                file.write(stale_fname)
            file.close()
            with open(ERRORS + fname_log + "_errors.log", "a") as file:
                file.write(f"post_rawcooked {date}: Matroka {stale_fname} encoding stalled mid_process.")
                file.write(f"    Matrosk and failed log deleted. DPX sequence will retry RAWCooked encoding.")
                file.write(
                    f"    Please contact the Knowledge and Collections Developer if this item repeatedly stalls.")
            file.close()

# Add list of stalled logs to post_rawcooked.log
if os.path.getsize(stale_encodings_file) > 0:
    log(logfile, "Stalled files that will be deleted:")
    with open(stale_encodings_file, 'r') as input_file, open(logfile, 'a') as output_file:
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

# Update the count of successful cooks at top of the success log
# First create new temp_success_log with timestamp
with open(MKV_DESTINATION + "temp_rawcooked_success.log", "w") as output_file:
    output_file.write(f"===================== Updated ===================== {date}")
    # Count lines in success_log and create count variable, output that count to new success log, then output all lines with /home* to the new log
    with open(MKV_DESTINATION + "rawcooked_success.log", "r") as input_file:
        lines = input_file.readlines()
        success_count = len(lines)
        for line in lines:
            output_file.write(line)
    input_file.close()
    output_file.write(f"===================== Successful cooks: {success_count} ===================== {date}")
output_file.close()

# Sort the log and remove any non-unique lines
with open(MKV_DESTINATION + "temp_rawcooked_success.log", "r") as file:
    lines = file.readlines()
    unique = list(set(lines))
    unique.sort()
file.close()

with open(MKV_DESTINATION + "temp_rawcooked_success_unique.log", "w") as file:
    file.writelines(unique)

for filename in file_names:
    os.remove(filename)

shutil.move(MKV_DESTINATION + "temp_rawcooked_success_unique.log", MKV_DESTINATION + "rawcooked_success.log")

