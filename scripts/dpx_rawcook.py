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
def rawcooked_command_executor(start_folder_path: str, mkv_file_name: str, md5_checksum: bool = False,
                               v2: bool = False):
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
        self.rawcooked_v1_success_log = os.path.join(MKV_DEST, 'rawcooked_dpx_v1_success.log')
        self.rawcooked_v2_success_log = os.path.join(MKV_DEST, 'rawcooked_dpx_v2_success.log')

        self.temp_rawcooked_v1_file = os.path.join(MKV_DEST, "temp_rawcooked_v1_list.txt")
        self.temp_rawcooked_v2_file = os.path.join(MKV_DEST, "temp_rawcooked_v2_list.txt")

        self.file_names = [self.temp_rawcooked_v1_file, self.temp_rawcooked_v2_file]

    def process(self):
        if not os.path.exists(self.logfile):
            with open(self.logfile, 'w+'):
                pass

        if not os.path.exists(self.rawcooked_v1_success_log):
            with open(self.rawcooked_v1_success_log, 'w+'):
                pass

        if not os.path.exists(self.rawcooked_v2_success_log):
            with open(self.rawcooked_v2_success_log, 'w+'):
                pass

        # create the temporary files
        for file_name in self.file_names:
            with open(file_name, 'w') as f:
                pass

        # Write a START note to the logfile if files for encoding, else exit
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
        # If execution stops, we can see the sequences that were cooked
        with open(self.temp_rawcooked_v2_file, 'a+') as file:
            for seq_path in dpx_to_cook.keys():
                file.write(f"{seq_path}\n")
                logging_utils.log(self.logfile, f"{seq_path} will be cooked using RAWCooked V2")

                mkv_file_name = os.path.basename(seq_path)

                with concurrent.futures.ProcessPoolExecutor() as executor:
                    executor.submit(rawcooked_command_executor, seq_path, mkv_file_name, False, False)

                # Cooking with --framemd5 flag
                with concurrent.futures.ProcessPoolExecutor() as executor:
                    executor.submit(rawcooked_command_executor, seq_path, mkv_file_name, True, False)

    # ========================
    # === RAWcook pass two ===
    # ========================
    # Cooks with output v1
    def pass_two(self):

        logging_utils.log(self.logfile, "Checking for files to cook using RAWCooked V1")
        sequence_map = find_dpx_folder_from_sequence(DPX_PATH)

        if len(sequence_map) == 0:
            logging_utils.log(self.logfile, "No sequence found to be cooked with RAWCooked V1")
            return

        # Taking only 20 entries from the dictionary
        dpx_to_cook = dict(itertools.islice(sequence_map.items(), 20))

        print(dpx_to_cook)

        # Store the paths in a temporary .txt file
        # If execution stops, we can see the sequences that were cooked
        with open(self.temp_rawcooked_v1_file, 'a+') as file:
            for seq_path in dpx_to_cook.keys():
                file.write(f"{seq_path}\n")
                logging_utils.log(self.logfile, f"{seq_path} will be cooked using RAWCooked V2")

                mkv_file_name = os.path.basename(seq_path)

                with concurrent.futures.ProcessPoolExecutor() as executor:
                    executor.submit(rawcooked_command_executor, seq_path, mkv_file_name, False, True)

                # Cooking with --framemd5 flag
                with concurrent.futures.ProcessPoolExecutor() as executor:
                    executor.submit(rawcooked_command_executor, seq_path, mkv_file_name, True, True)

    def clean(self):

        with open(self.rawcooked_v1_success_log, 'a') as target:
            with open(self.temp_rawcooked_v1_file, 'r') as source:
                target.write(source.read())

        with open(self.rawcooked_v2_success_log, 'a') as target:
            with open(self.temp_rawcooked_v2_file, 'r') as source:
                target.write(source.read())

        # Clean up temporary files
        for file_name in self.file_names:
            if os.path.exists(file_name):
                os.remove(file_name)
                print(f"Deleted file: {file_name}")

    def execute(self):
        # TODO: Implement Error handling mechanisms
        self.process()
        self.pass_one()
        self.pass_two()
        self.clean()


if __name__ == '__main__':
    dpx_rawcook = DpxRawcook()
    dpx_rawcook.execute()
