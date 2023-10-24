import subprocess

from utils.find_utils import find_directories
from utils.logging_utils import log
import concurrent.futures
from dotenv import load_dotenv

import os
import re

from utils.shell_utils import run_command

load_dotenv()

# =========================================================================
# === RAWcook encoding script, two pass for --check and --check-padding ===
# =========================================================================

# Global variables extracted from environmental variables
SCRIPT_LOG = os.environ.get('FILM_OPS') + os.environ.get('DPX_SCRIPT_LOG')
DPX_PATH = os.environ.get('FILM_OPS') + os.environ.get('DPX_COOK')
MKV_DEST = os.environ.get('FILM_OPS') + os.environ.get('MKV_ENCODED')

logfile = "dpx_rawcook.log"

# Remove or generate temporary files per script run
temp_rawcooked_file = MKV_DEST + "temporary_rawcook_list.txt"
temp_retry_file = MKV_DEST + "temporary_retry_list.txt"
retry_file = MKV_DEST + "retry_list.txt"
rawcook_file = MKV_DEST + "rawcook_list.txt"
reversibility_file = MKV_DEST + "reversibility_list.txt"

queued_file = MKV_DEST + "temp_queued_list.txt"
cooked_folder = MKV_DEST + "mkv_cooked"

with open(queued_file, 'w') as file:
    file.write("\n".join(os.listdir(cooked_folder)))

file_names = [temp_rawcooked_file, temp_retry_file, retry_file, rawcook_file]

for file_name in file_names:
    with open(file_name, 'w') as f:
        pass


# Write a START note to the logfile if files for encoding, else exit
if os.path.isfile(reversibility_file):
    log(logfile, "============= DPX RAWcook script START =============")
elif not len(os.listdir(DPX_PATH)):
    print("No files available for encoding, script exiting")
else:
    log(logfile, "============= DPX RAWcook script START =============")

# ========================
# === RAWcook pass one ===
# ========================

# Run first pass where list generated for large reversibility cases by dpx_post_rawcook.sh
log(logfile, "Checking for files that failed RAWcooked due to large reversibility files")
with open(reversibility_file, 'r') as file:
    rev_file_list = file.read().splitlines()

for rev_file in rev_file_list:
    print(reversibility_file)
    print("+++++++++++++++++++++++++++++++++++++++++++++")
    folder_retry = os.path.basename(rev_file)
    count_cooked_2 = 0
    count_queued_2 = 0
    with open(MKV_DEST+"rawcooked_success.log", "r") as file:
        for line in file:
            count_cooked_2 += line.count(folder_retry)
    with open(queued_file, "r") as file:
        for line in file:
            count_queued_2 += line.count(folder_retry)

    # Those not already queued/active passed to list, else bypassed
    if count_cooked_2 == 0 and count_queued_2 == 0:
        with open(temp_retry_file, 'a') as file:
            file.write(folder_retry)

# Sort the temporary_rawcook_list by part of extension, pass first 20 to rawcook_list.txt
with open(temp_retry_file, 'r') as file:
    cook_retry = list(set(file.read().splitlines())).sort()
    print(cook_retry)

log(logfile, "DPX folder will be cooked using --output-version 2:") #TODO Change this dumb logic
if cook_retry:
    with open(retry_file, 'w') as file:
        file.writelines(item + "\n" for item in cook_retry if cook_retry)

    log(logfile, (item + "\n" for item in cook_retry if cook_retry))

# Begin RAWcooked processing with GNU Parallel using --output-version 2
command = f'cat "{MKV_DEST}retry_list.txt" | parallel --jobs 4 "rawcooked --license 00C5BAEDE01E98D64496F0 -y --all --no-accept-gaps --output-version 2 -s 5281680 ${DPX_PATH}{{}} -o ${MKV_DEST}mkv_cooked/{{}}.mkv &>> {MKV_DEST}mkv_cooked/{{}}.mkv.txt"'
subprocess.run(command, shell=True, check=True)
#TODO change above command to parallel jobs

# with open(retry_file, 'r') as file:
#     filenames = file.read().splitlines()
#
# with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:          #TODO Change to parallel processes rather than threads
#     futures = [executor.submit(run_command, command, "{}") for argument in filenames]
#     concurrent.futures.wait(futures)

# os.remove(reversibility_file)

# ========================
# === RAWcook pass two ===
# ========================

# Refresh temporary queued list
temp_queued_list = os.listdir(cooked_folder)
with open(queued_file, 'w') as file:
    file.write("\n".join(temp_queued_list))

# When large reversibility cooks complete target all N_ folders, and pass any not already being processed to temporary_rawcook_list.txt
log(logfile, "Outputting files from DPX_PATH to list, if not already queued")
folders = find_directories(DPX_PATH, 1)
for folder in folders:
    name = folder.split('/')[-1]
    if name.startswith("N_"):
        folder_clean = os.path.basename(folder)
        count_cooked = 0
        count_queued = 0
        with open(MKV_DEST+"rawcooked_success.log", "r") as file:
            for line in file:
                count_cooked += line.count(folder_clean)
        with open(queued_file, "r") as file:
            for line in file:
                count_queued += line.count(folder_clean)
        if count_cooked == 0 and count_queued == 0:
            print(folder_clean)
            with open(temp_rawcooked_file, 'a') as file:
                file.write(folder_clean)

cook_list = []
# Sort the temporary_rawcook_list by part of extension, pass first 20 to rawcook_list.txt and write items to log
with open(temp_rawcooked_file, 'r') as file:
    for line in file:
        if line.startswith("N_"):
            cook_list.append(line)

log(logfile, "DPX folder will be cooked:")  #TODO Change this dumb logic
if cook_list is not None:
    cook_list.sort()
    cook_list = list(set(cook_list))[0:20]
    with open(rawcook_file, 'w') as file:
        file.write("\n".join(cook_list))

    log(logfile, "\n".join(cook_list))


# Begin RAWcooked processing with GNU Parallel
command = f'cat "{MKV_DEST}rawcook_list.txt" | parallel --jobs 4 "rawcooked --license 00C5BAEDE01E98D64496F0 -y --all --no-accept-gaps -s 5281680 {DPX_PATH}{{}} -o {MKV_DEST}mkv_cooked/{{}}.mkv &>> {MKV_DEST}mkv_cooked/{{}}.mkv.txt"'
subprocess.run(command, shell=True, check=True)
#TODO change above command to parallel jobs

# with open (rawcook_file,'r') as file:
#     filenames = file.read().splitlines()
#     print(filenames)
#
#     with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:          #Change to parallel processes rather than threads
#         futures = [executor.submit(run_command, command, "{}") for argument in filenames]
#         concurrent.futures.wait(futures)


command = f'cat "{MKV_DEST}rawcook_list.txt" | parallel --jobs 4 "rawcooked --license 00C5BAEDE01E98D64496F0 -y --all --no-accept-gaps -s 5281680 --framemd5 {DPX_PATH}{{}} -o {MKV_DEST}mkv_cooked/{{}}.mkv &>> {MKV_DEST}mkv_cooked/{{}}.mkv.txt"'
subprocess.run(command, shell=True, check=True)
#TODO change above command to parallel jobs

# with open (rawcook_file,'r') as file:
#     filenames = file.read().splitlines()
#
#     with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:          #Change to parallel processes rather than threads
#         futures = [executor.submit(run_command, command, "{}") for argument in filenames]
#         concurrent.futures.wait(futures)

log(logfile, "===================== DPX RAWcook ENDED =====================")

# Clean up temporary files
for file_name in file_names:
    if os.path.exists(file_name):
        os.remove(file_name)
        print(f"Deleted file: {file_name}")
    else:
        print(f"File not found: {file_name}")