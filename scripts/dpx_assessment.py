from utils.logging_utils import log
from utils.find_utils import find_directories
from utils.find_utils import find_in_logs
from utils.shell_utils import get_media_info
from utils.shell_utils import generate_tree
from utils.shell_utils import check_mediaconch_policy
from utils.shell_utils import run_script
from utils.split_utils import sort_split_list
from utils.split_utils import create_python_list
from dotenv import load_dotenv

import os
import re
import concurrent.futures

# Load environment variables from .env file
load_dotenv()

SCRIPT_LOG = os.environ.get('FILM_OPS') + os.environ.get('DPX_SCRIPT_LOG')
DPX_PATH = os.environ.get('FILM_OPS') + os.environ.get('DPX_ASSESS')
PY3_LAUNCH = os.environ.get('PY3_ENV')
SPLITTING = os.environ.get('SPLITTING_SCRIPT_FILMOPS')
POLICY_PATH = os.environ.get('FILM_OPS') + os.environ.get('POLICY_DPX')

logfile = SCRIPT_LOG + "dpx_assessment.log"
rawcooked_dpx_file = DPX_PATH + 'rawcooked_dpx_list.txt'
luma_4k_dpx_file = DPX_PATH + 'luma_4k_dpx_list.txt'
tar_dpx_file = DPX_PATH + 'tar_dpx_list.txt'
python_file = DPX_PATH + "python_list.txt"
success_file = DPX_PATH + "rawcook_dpx_success.log"
failure_file = DPX_PATH + "tar_dpx_failures.log"

# Check for DPX sequences in path before script launch
if not os.listdir(DPX_PATH):  # Make try catch
    print("No files available for encoding, script exiting")
    exit(1)
else:
    log(logfile, "============= DPX Assessment workflow START =============")

# Refresh temporary success/failure lists
file_names = [rawcooked_dpx_file, tar_dpx_file, luma_4k_dpx_file, python_file]

# Creating files from the file_names list
for file_name in file_names:
    with open(file_name, 'w') as f:
        pass

    print(f"Created file: {file_name}")

# Loop that retrieves single DPX file in each folder, runs Mediaconch check and generates metadata files
# Configured for three level folders: N_123456_01of01/scan01/dimensions/<dpx_seq>
depth = 3  # Take as user input
dirs = find_directories(DPX_PATH, depth)

# Checking mediaconch policy for first DPX file in each folder
for dir_name in dirs:
    dpx = os.listdir(dir_name)[0]
    components = dir_name.split('/')
    dimensions = components[-1]
    scans = components[-2]
    filename = components[-3]
    file_scan_name = filename + '/' + scans
    queued_pass = find_in_logs(DPX_PATH + "rawcook_dpx_success.log", file_scan_name)
    queued_fail = find_in_logs(DPX_PATH + "tar_dpx_failures.log", file_scan_name)

    if not queued_pass and not queued_fail:
        log(logfile, "Metadata file creation has started for:")
        log(logfile, file_scan_name + '/' + dimensions + '/' + dpx)
        mediainfo_output_file = DPX_PATH + file_scan_name + '/' + filename + '_' + dpx + '_metadata.txt'
        tree_output_file = DPX_PATH + file_scan_name + '/' + filename + '_directory_contents.txt'
        size_output_file = DPX_PATH + file_scan_name + '/' + filename + '_directory_total_byte_size.txt'
        get_media_info('-f', dir_name + '/' + dpx, mediainfo_output_file)
        generate_tree(dir_name, '', tree_output_file)  # Check if we need this tree
        byte_size = os.path.getsize(DPX_PATH + filename)
        with open(size_output_file, 'w') as file:
            file.write(filename + 'total folder size in bytes (du -s -b from BK-CI-DATA3): ' + str(
                byte_size))  # check if we need this wording

        # Start comparison of first dpx file against mediaconch policy
        check = check_mediaconch_policy(POLICY_PATH, dir_name + '/' + dpx)
        check_str = check.stdout.decode()

        if check_str.startswith('pass!'):
            media_info = get_media_info('--Details=1', dir_name + '/' + dpx)
            search_term = "Pixels per line:"
            pixel_matches = [line for line in media_info.splitlines() if search_term.lower() in line.lower()]
            pixels_per_line = int(re.split(r"\s+", pixel_matches[0])[4])
            if pixels_per_line > 3999:
                log(logfile,
                    "PASS: 4K scan" + file_scan_name + "has passed the MediaConch policy and can progress to RAWcooked processing path")
                with open(luma_4k_dpx_file, 'w') as file:
                    file.write(DPX_PATH + filename)
            else:
                search_term = "Descriptor"
                descriptor_matches = [line for line in media_info.splitlines() if search_term.lower() in line.lower()]
                luma_match = descriptor_matches[0].find("Luma (Y)")
                if luma_match > 0:
                    log(logfile,
                        "PASS: Luma (Y) $file_scan_name has passed the MediaConch policy and can progress to RAWcooked processing path")
                    with open(luma_4k_dpx_file, 'w') as file:
                        file.write(DPX_PATH + filename)
                else:
                    log(logfile,
                        "PASS: RGB $file_scan_name has passed the MediaConch policy and can progress to RAWcooked processing path")
                    with open(rawcooked_dpx_file, 'w') as file:
                        file.write(DPX_PATH + filename)

        else:
            log(logfile,
                "FAIL: " + file_scan_name + " DOES NOT CONFORM TO MEDIACONCH POLICY. Adding to tar_dpx_failures_list.txt")
            log(logfile, check_str)
            with open(tar_dpx_file, 'w') as file:
                file.write(DPX_PATH + filename)

    else:
        log(logfile, "SKIPPING DPX folder, it has already been processed but has not moved to correct processing path:")
        log(logfile, file_scan_name)

# Prepare luma_4k_dpx_list for DPX splitting script/move to RAWcooked preservation
luma_4k_list = sort_split_list(luma_4k_dpx_file)
if luma_4k_list:
    log(logfile, "RAWcooked Luma Y/4K path items for size check and Python splitting/moving script:")
    log(logfile, "\n".join(luma_4k_list))
    luma_4k_size_dict = create_python_list(luma_4k_dpx_file, python_file, 'luma_4k')
    for key in luma_4k_size_dict:
        log(logfile, "Size of " + key + " is " + luma_4k_size_dict[key] + " KB. Passing to Python script...")

# Prepare tar_dpx_failure_list for DPX splitting script/move to TAR preservation
tar_dpx_failure_list = sort_split_list(tar_dpx_file)
if tar_dpx_failure_list:
    log(logfile, "TAR path items for size check and Python splitting/moving script:")
    log(logfile, "\n".join(tar_dpx_failure_list))
    tar_size_dict = create_python_list(tar_dpx_file, python_file, 'tar')
    for key in tar_size_dict:
        log(logfile, "Size of " + key + " is " + tar_size_dict[key] + " KB. Passing to Python script...")

# Prepare dpx_success_list for DPX splitting script/move to TAR preservation
dpx_success_list = sort_split_list(rawcooked_dpx_file)
if dpx_success_list:
    log(logfile, "RAWcooked 2K RGB path items for size check and Python splitting/moving script:")
    log(logfile, "\n".join(dpx_success_list))
    rawcooked_size_dict = create_python_list(rawcooked_dpx_file, python_file, 'rawcooked')
    for key in rawcooked_size_dict:
        log(logfile, "Size of " + key + " is " + rawcooked_size_dict[key] + " KB. Passing to Python script...")

if os.path.getsize(python_file) > 0:
    log(logfile,
        "Launching python script to process DPX sequences. Please see dpx_splitting_script.log for more details")
    with open(python_file, 'r') as file:
        file_list = set(file.read().splitlines())
    with concurrent.futures.ThreadPoolExecutor(
            max_workers=1) as executor:  # Change to parallel processes rather than threads
        futures = [executor.submit(run_script, PY3_LAUNCH, SPLITTING, argument) for argument in file_list]
        concurrent.futures.wait(futures)

    log(logfile, "===================== DPX assessment workflows ENDED =====================")

# Append latest pass/failures to movement logs
with open(success_file, 'a') as target:
    with open(rawcooked_dpx_file, 'r') as source:
        target.write(source.read())
    source.close()
    with open(luma_4k_dpx_file, 'r') as source:
        target.write(source.read())
    source.close()
target.close()

with open(failure_file, 'a') as target:
    with open(tar_dpx_file, 'r') as source:
        target.write(source.read())
    source.close()
target.close()

# Clean up temporary files
for file_name in file_names:
    if os.path.exists(file_name):
        os.remove(file_name)
        print(f"Deleted file: {file_name}")
    else:
        print(f"File not found: {file_name}")
