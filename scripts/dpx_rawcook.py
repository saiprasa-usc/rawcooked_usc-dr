import subprocess
from utils.find_utils import find_directories
from utils.logging_utils import log
import concurrent.futures
from dotenv import load_dotenv

import os

load_dotenv()

# =========================================================================
# === RAWcook encoding script, two pass for --check and --check-padding ===
# =========================================================================

# Global variables extracted from environmental variables
SCRIPT_LOG = os.path.join(os.environ.get('FILM_OPS'), os.environ.get('DPX_SCRIPT_LOG'))
DPX_PATH = os.path.join(os.environ.get('FILM_OPS'), os.environ.get('DPX_COOK'))
MKV_DEST = os.path.join(os.environ.get('FILM_OPS'), os.environ.get('MKV_ENCODED'))

SCRIPT_LOG = r'{}'.format(SCRIPT_LOG)
DPX_PATH = r'{}'.format(DPX_PATH)
MKV_DEST = r'{}'.format(MKV_DEST)

def process_mkv(line, md5_checksum=False):
    # By observation, any rawcooked failed will result return code 0 but message is captured in stderr
    code = f"rawcooked --license 00C5BAEDE01E98D64496F0 -y --all --no-accept-gaps -s 5281680 {'--framemd5' if md5_checksum else ''} {DPX_PATH}{line} -o {MKV_DEST}mkv_cooked/{line}.mkv &>> {MKV_DEST}mkv_cooked/{line}.mkv.txt"
    p = subprocess.run(code, shell=True, check=True, stderr=subprocess.PIPE)
    return "CODE", p.returncode, p.stderr


def process_mkv_output_v2(line):
    code = f"rawcooked --license 00C5BAEDE01E98D64496F0 -y --all --no-accept-gaps --output-version 2 -s 5281680 {DPX_PATH}{line} -o {MKV_DEST}mkv_cooked/{line}.mkv &>> {MKV_DEST}mkv_cooked/{line}.mkv.txt"
    p = subprocess.run(code, shell=True, check=True, stderr=subprocess.PIPE)
    return "CODE", p.returncode, p.stderr


class DpxRawcook:

    def __init__(self):
        self.logfile = os.path.join(SCRIPT_LOG, "dpx_rawcook.log")
        self.temp_rawcooked_file = os.path.join(MKV_DEST, "temporary_rawcook_list.txt")
        self.temp_retry_file = os.path.join(MKV_DEST, "temporary_retry_list.txt")
        self.retry_file = os.path.join(MKV_DEST, "retry_list.txt")
        self.rawcook_file = os.path.join(MKV_DEST, "rawcook_list.txt")
        self.reversibility_file = os.path.join(MKV_DEST, "reversibility_list.txt")
        self.queued_file = os.path.join(MKV_DEST, "temp_queued_list.txt")
        self.cooked_folder = os.path.join(MKV_DEST, "mkv_cooked")

        self.file_names = [self.temp_rawcooked_file, self.temp_retry_file, self.retry_file, self.rawcook_file]

    def process(self):
        # Reading files from mkv_cooked folder and writing it to temp_queued_list
        with open(self.queued_file, 'w') as file:
            file.write("\n".join(os.listdir(self.cooked_folder)))

        # create the temporary files
        for file_name in self.file_names:
            with open(file_name, 'w') as f:
                pass

        if not os.path.exists(self.reversibility_file):
            with open(self.reversibility_file, 'w') as f:
                pass

        # Write a START note to the logfile if files for encoding, else exit
        if os.path.isfile(self.reversibility_file):
            log(self.logfile, "============= DPX RAWcook script START =============")
        elif not len(os.listdir(DPX_PATH)):
            print("No files available for encoding, script exiting")
        else:
            log(self.logfile, "============= DPX RAWcook script START =============")

    # ========================
    # === RAWcook pass one ===
    # ========================

    def pass_one(self):
        # Run first pass where list generated for large reversibility cases by dpx_post_rawcook.sh
        log(self.logfile, "Checking for files that failed RAWcooked due to large reversibility files")
        with open(self.reversibility_file, 'r') as file:
            rev_file_list = file.read().splitlines()

        for rev_file in rev_file_list:
            print(self.reversibility_file)
            print("+++++++++++++++++++++++++++++++++++++++++++++")
            folder_retry = os.path.basename(rev_file)
            count_cooked_2 = 0
            count_queued_2 = 0
            with open(MKV_DEST + "rawcooked_success.log", 'r') as file:
                for line in file:
                    count_cooked_2 += line.count(folder_retry)
            with open(self.queued_file, 'r') as file:
                for line in file:
                    count_queued_2 += line.count(folder_retry)

            # Those not already queued/active passed to list, else bypassed
            if count_cooked_2 == 0 and count_queued_2 == 0:
                with open(self.temp_retry_file, 'a') as file:
                    file.write(folder_retry)

        # Sort the temporary_rawcook_list by part of extension, pass first 20 to rawcook_list.txt
        with open(self.temp_retry_file, 'r') as file:
            cook_retry = list(set(file.read().splitlines()))
            cook_retry.sort()


            log(self.logfile, "DPX folder will be cooked using --output-version 2:")  # TODO Change this dumb logic
            if cook_retry and len(cook_retry) > 0:
                with open(self.retry_file, 'w') as file:
                    file.writelines(item + "\n" for item in cook_retry if cook_retry)

                log(self.logfile, (item + "\n" for item in cook_retry if cook_retry))


        with open(f'{MKV_DEST}retry_list.txt') as retry_list:
            for file_name in retry_list:
                with concurrent.futures.ProcessPoolExecutor() as executor:
                    f0 = executor.submit(process_mkv_output_v2, file_name)


        # # Begin RAWcooked processing with GNU Parallel using --output-version 2
        # command = f'cat "{MKV_DEST}retry_list.txt" | parallel --jobs 4 "rawcooked --license 00C5BAEDE01E98D64496F0 -y --all --no-accept-gaps --output-version 2 -s 5281680 ${DPX_PATH}{{}} -o ${MKV_DEST}mkv_cooked/{{}}.mkv &>> {MKV_DEST}mkv_cooked/{{}}.mkv.txt"'
        # process = subprocess.run(command, shell=True, capture_output=True)
        # # TODO change above command to parallel jobs

    # ========================
    # === RAWcook pass two ===
    # ========================

    def pass_two(self):
        # Refresh temporary queued list
        temp_queued_list = os.listdir(self.cooked_folder)
        with open(self.queued_file, 'w') as file:
            file.write("\n".join(temp_queued_list))

        # When large reversibility cooks complete target all N_ folders, and pass any not already being processed to
        # temporary_rawcook_list.txt
        log(self.logfile, "Outputting files from DPX_PATH to list, if not already queued")
        folders = find_directories(DPX_PATH, 1)
        for folder in folders:

            name = folder.split('/')[-1]
            if name is not None:
                folder_clean = os.path.basename(folder)

                count_cooked = 0
                count_queued = 0
                with open(MKV_DEST + "rawcooked_success.log", "r") as file:
                    for line in file:
                        count_cooked += line.count(folder_clean)
                with open(self.queued_file, "r") as file:
                    for line in file:
                        count_queued += line.count(folder_clean)
                    print(count_queued)
                if count_cooked == 0 and count_queued == 0:
                    with open(self.temp_rawcooked_file, 'a') as file:
                        file.write(folder_clean)

        cook_list = []

        # Sort the temporary_rawcook_list by part of extension, pass first 20 to rawcook_list.txt and write items to log
        with open(self.temp_rawcooked_file, 'r') as file:
            for line in file:
                if line is not None:
                    cook_list.append(line)

        log(self.logfile, "DPX folder will be cooked:")  # TODO Change this dumb logic
        if cook_list is not None:
            cook_list.sort()
            cook_list = list(set(cook_list))[0:20]
            with open(self.rawcook_file, 'w') as file:
                file.write("\n".join(cook_list))

            log(self.logfile, "\n".join(cook_list))

        # Begin RAWcooked processing with GNU Parallel

        path = f'{MKV_DEST}rawcook_list.txt'
        path = r'{}'.format(path)
        with open(path) as cook_list:
            for file_name in cook_list:
                print("DEBUG", file_name)
                with concurrent.futures.ProcessPoolExecutor() as executor:
                    f1 = executor.submit(process_mkv, file_name, False)

        # command = (f'cat "{MKV_DEST}rawcook_list.txt" | parallel --jobs 4 "rawcooked --license
        # 00C5BAEDE01E98D64496F0 -y --all ' f'--no-accept-gaps -s 5281680 {DPX_PATH}{{}} -o {MKV_DEST}mkv_cooked/{{
        # }}.mkv &>> {MKV_DEST}mkv_cooked/{{' f'}}.mkv.txt"') process = subprocess.run(command, shell=True,
        # capture_output=True) TODO change above command to parallel jobs

        with open(path) as cook_list:
            for file_name in cook_list:
                with concurrent.futures.ProcessPoolExecutor() as executor:
                    f2 = executor.submit(process_mkv, file_name, True)


        # command = f'cat "{MKV_DEST}rawcook_list.txt" | parallel --jobs 4 "rawcooked --license
        # 00C5BAEDE01E98D64496F0 -y --all --no-accept-gaps -s 5281680 --framemd5 {DPX_PATH}{{}} -o {
        # MKV_DEST}mkv_cooked/{{}}.mkv &>> {MKV_DEST}mkv_cooked/{{}}.mkv.txt"' process = subprocess.run(command,
        # shell=True, capture_output=True)

        # TODO change above command to parallel jobs

        log(self.logfile, "===================== DPX RAWcook ENDED =====================")

    def clean(self):
        # Clean up temporary files
        for file_name in self.file_names:
            if os.path.exists(file_name):
                os.remove(file_name)
                print(f"Deleted file: {file_name}")
            else:
                print(f"File not found: {file_name}")

    def execute(self):
        # TODO: Implement Error handling mechanisms
        # TODO: Clean unnecessary print statements
        self.process()
        self.pass_one()
        self.pass_two()
        self.clean()


dpx_rawcook = DpxRawcook()
dpx_rawcook.execute()
