import concurrent.futures
import itertools
import os
import subprocess

from dotenv import load_dotenv

from utils import find_utils, logging_utils

load_dotenv()

# =========================================================================
# === RAWcook encoding script, two pass for --check and --check-padding ===
# =========================================================================

# Global variables extracted from environmental variables
SCRIPT_LOG = os.path.join(os.environ.get('FILM_OPS'), os.environ.get('DPX_SCRIPT_LOG'))
DPX_PATH = os.path.join(os.environ.get('FILM_OPS'), os.environ.get('DPX_COOK'))
MKV_DEST = os.path.join(os.environ.get('FILM_OPS'), os.environ.get('MKV_ENCODED'))
DPX_V2_PATH = os.path.join(os.environ.get('FILM_OPS'), os.environ.get('DPX_COOK_V2'))

SCRIPT_LOG = r'{}'.format(SCRIPT_LOG)
DPX_PATH = r'{}'.format(DPX_PATH)
DPX_V2_PATH = r'{}'.format(DPX_V2_PATH)
MKV_DEST = r'{}'.format(MKV_DEST)


# It will not create the .mkv.txt file as Popen() will dump the output in realtime to the console
def process_mkv(start_folder_path: str, mkv_file_name: str, md5_checksum: bool = False, v2: bool = False):
    # By observation, any rawcooked failed will result return code 0 but message is captured in stderr code =
    # f"rawcooked --license 00C5BAEDE01E98D64496F0 -y --all --no-accept-gaps -s 5281680 {'--framemd5' if md5_checksum
    # else ''} {DPX_PATH}{line} -o {MKV_DEST}mkv_cooked/{line}.mkv &>> {MKV_DEST}mkv_cooked/{line}.mkv.txt"

    string_command = f"rawcooked --license 004B159A2BDB07331B8F2FDF4B2F -y --all --no-accept-gaps -s 5281680 {'--framemd5' if md5_checksum else ''} {start_folder_path} -o {MKV_DEST}mkv_cooked/{mkv_file_name}.mkv"
    output_txt_file = f"{MKV_DEST}mkv_cooked/{mkv_file_name}.mkv.txt"
    command = string_command.split(" ")
    command = [c for c in command if len(c) > 0]
    command = list(command)
    print(command)
    subprocess_logs = []
    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as p:
        for line in p.stderr:
            subprocess_logs.append(line)
            print(f"{mkv_file_name} : {line}")
        for line in p.stdout:
            subprocess_logs.append(line)
            print(f"{mkv_file_name} : {line}")

    std_logs = ''.join(subprocess_logs)
    with open(output_txt_file, 'a+') as file:
        file.write(std_logs)


def process_mkv_output_v2(line):
    code = f"rawcooked --license 004B159A2BDB07331B8F2FDF4B2F -y --all --no-accept-gaps --output-version 2 -s 5281680 {DPX_PATH}{line} -o {MKV_DEST}mkv_cooked/{line}.mkv &>> {MKV_DEST}mkv_cooked/{line}.mkv.txt"
    p = subprocess.run(code, shell=True, check=True, stderr=subprocess.PIPE)
    return "CODE", p.returncode, p.stderr


def find_dpx_folder_from_sequence(dpx_folder_path) -> dict:
    dpx_to_cook = {}
    for seq in os.listdir(dpx_folder_path):
        seq_path = os.path.join(dpx_folder_path, seq)
        if os.path.isfile(seq_path) and not seq.endswith('.dpx'):
            continue

        for dir_path, dir_names, file_names in os.walk(seq_path):
            if len(file_names) == 0:
                continue
            for file_name in file_names:
                if file_name.endswith('.dpx'):
                    dpx_to_cook[seq_path] = dir_path
                    break

    return dpx_to_cook


class DpxRawcook:

    def __init__(self):
        self.logfile = os.path.join(SCRIPT_LOG, "dpx_rawcook.log")
        self.rawcooked_success_log = os.path.join(MKV_DEST, 'rawcooked_success.log')
        self.temp_rawcooked_file = os.path.join(MKV_DEST, "temp_rawcook_list.txt")
        self.temp_rawcook_v2_file = os.path.join(MKV_DEST, "temp_rawcook_v2_list.txt")
        self.retry_file = os.path.join(MKV_DEST, "retry_list.txt")
        self.rawcook_file = os.path.join(MKV_DEST, "rawcook_list.txt")
        self.reversibility_file = os.path.join(MKV_DEST, "reversibility_list.txt")
        self.queued_file = os.path.join(MKV_DEST, "temp_queued_list.txt")
        self.cooked_folder = os.path.join(MKV_DEST, "mkv_cooked")

        self.file_names = [self.temp_rawcooked_file, self.temp_rawcook_v2_file, self.retry_file, self.rawcook_file]

    def process(self):

        if not os.path.exists(self.logfile):
            with open(self.logfile, 'w+'):
                pass

        if not os.path.exists(self.rawcooked_success_log):
            with open(self.rawcooked_success_log, 'w+'):
                pass

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
            logging_utils.log(self.logfile, "============= DPX RAWcook script START =============")
        elif not len(os.listdir(DPX_PATH)):
            print("No files available for encoding, script exiting")
        else:
            logging_utils.log(self.logfile, "============= DPX RAWcook script START =============")

    # ========================
    # === RAWcook pass one ===
    # ========================

    # Cooks with output v2
    def pass_one(self):
        # Run first pass where list generated for large reversibility cases by dpx_post_rawcook.sh
        logging_utils.log(self.logfile, "Checking for files that failed RAWcooked due to large reversibility files")

        # <sequence path, dpx_folder_path> pairs
        sequence_map = find_dpx_folder_from_sequence(DPX_V2_PATH)

        if len(sequence_map) == 0:
            logging_utils.log(self.logfile, "No sequence found to be cooked with RAWCooked V2")
            return

        # Taking only 20 entries from the dictionary
        dpx_to_cook = dict(itertools.islice(sequence_map.items(), 20))

        # Store the paths in a temporary .txt file
        # If execution stops, we can see the files that were cooked
        with open(self.temp_rawcook_v2_file, 'a+') as file:
            for seq_path in dpx_to_cook.keys():
                file.write(f"{seq_path}\n")
                logging_utils.log(self.logfile, f"{seq_path} will be cooked using RAWCooked V2")

                rawcook_start_folder = dpx_to_cook.get(seq_path)
                mkv_file_name = os.path.basename(seq_path)

                with concurrent.futures.ProcessPoolExecutor() as executor:
                    executor.submit(process_mkv, rawcook_start_folder, mkv_file_name, False, True)

                # Cooking with --framemd5 flag
                with concurrent.futures.ProcessPoolExecutor() as executor:
                    executor.submit(process_mkv, rawcook_start_folder, mkv_file_name, True, True)

    # ========================
    # === RAWcook pass two ===
    # ========================

    # Cooks with output v1
    def pass_two(self):
        # Refresh temporary queued list
        temp_queued_list = os.listdir(self.cooked_folder)
        with open(self.queued_file, 'w') as file:
            file.write("\n".join(temp_queued_list))

        # When large reversibility cooks complete target all N_ folders, and pass any not already being processed to
        # temporary_rawcook_list.txt
        logging_utils.log(self.logfile, "Outputting files from DPX_PATH to list, if not already queued")
        folders = find_utils.find_directories(DPX_PATH, 1)
        folders = [f for f in folders if f != DPX_PATH]
        print("FOLDERS:", folders)
        for folder in folders:
            name = folder.split('/')[-1]
            if name is not None:
                folder_clean = os.path.basename(folder)
                print("NAME:", folder_clean)
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
                        file.write(f"{folder_clean}\n")

        cook_list = []

        # Sort the temporary_rawcook_list by part of extension, pass first 20 to rawcook_list.txt and write items to log
        with open(self.temp_rawcooked_file, 'r') as file:
            for line in file:
                if line is not None:
                    cook_list.append(line)

        logging_utils.log(self.logfile, "DPX folder will be cooked:")  # TODO Change this dumb logic
        if cook_list is not None:
            cook_list.sort()
            print(cook_list)
            cook_list = list(set(cook_list))[0:20]
            with open(self.rawcook_file, 'w') as file:
                for file_name in cook_list:
                    file.write(file_name)

            logging_utils.log(self.logfile, "".join(cook_list))

        # Begin RAWcooked processing with GNU Parallel

        path = f'{MKV_DEST}rawcook_list.txt'
        path = r'{}'.format(path)
        with open(path) as cook_list:
            for file_name in cook_list:
                print("DEBUG", file_name)
                with concurrent.futures.ProcessPoolExecutor() as executor:
                    f1 = executor.submit(process_mkv, file_name, False, False)

        # command = (f'cat "{MKV_DEST}rawcook_list.txt" | parallel --jobs 4 "rawcooked --license
        # 00C5BAEDE01E98D64496F0 -y --all ' f'--no-accept-gaps -s 5281680 {DPX_PATH}{{}} -o {MKV_DEST}mkv_cooked/{{
        # }}.mkv &>> {MKV_DEST}mkv_cooked/{{' f'}}.mkv.txt"') process = subprocess.run(command, shell=True,
        # capture_output=True) TODO change above command to parallel jobs

        with open(path) as cook_list:
            for file_name in cook_list:
                with concurrent.futures.ProcessPoolExecutor() as executor:
                    f2 = executor.submit(process_mkv, file_name, True, False)
        # command = f'cat "{MKV_DEST}rawcook_list.txt" | parallel --jobs 4 "rawcooked --license
        # 00C5BAEDE01E98D64496F0 -y --all --no-accept-gaps -s 5281680 --framemd5 {DPX_PATH}{{}} -o {
        # MKV_DEST}mkv_cooked/{{}}.mkv &>> {MKV_DEST}mkv_cooked/{{}}.mkv.txt"' process = subprocess.run(command,
        # shell=True, capture_output=True)

        # TODO change above command to parallel jobs

        logging_utils.log(self.logfile, "===================== DPX RAWcook ENDED =====================")

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
        # self.pass_two()
        self.clean()


if __name__ == '__main__':
    dpx_rawcook = DpxRawcook()
    dpx_rawcook.execute()
