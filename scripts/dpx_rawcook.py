import concurrent.futures
import itertools
import os
import shutil
import subprocess

from dotenv import load_dotenv

from utils import logging_utils

load_dotenv()

# =========================================================================
# === RAWcook encoding script, two pass for --check and --check-padding ===
# =========================================================================

# Global variables extracted from environmental variables
SCRIPT_LOG = os.path.join(os.environ.get('FILM_OPS'), os.environ.get('DPX_SCRIPT_LOG'))
DPX_PATH = os.path.join(os.environ.get('FILM_OPS'), os.environ.get('DPX_COOK'))
MKV_DEST = os.path.join(os.environ.get('FILM_OPS'), os.environ.get('MKV_ENCODED'))
DPX_V2_PATH = os.path.join(os.environ.get('FILM_OPS'), os.environ.get('DPX_COOK_V2'))
DPX_FOR_REVIEW_PATH = os.path.join(os.environ.get('FILM_OPS'), os.environ['DPX_REVIEW'])

SCRIPT_LOG = r'{}'.format(SCRIPT_LOG)
DPX_PATH = r'{}'.format(DPX_PATH)
DPX_V2_PATH = r'{}'.format(DPX_V2_PATH)
MKV_DEST = r'{}'.format(MKV_DEST)


# It will not create the .mkv.txt file as Popen() will dump the output in realtime to the console

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
        self.review_dpx_failure_log = os.path.join(MKV_DEST, 'review_dpx_failure.log')

        self.temp_rawcooked_v1_file = os.path.join(MKV_DEST, "temp_rawcooked_v1_list.txt")
        self.temp_rawcooked_v2_file = os.path.join(MKV_DEST, "temp_rawcooked_v2_list.txt")
        self.temp_review_file = os.path.join(MKV_DEST, "temp_review_list.txt")

        self.file_names = [self.temp_rawcooked_v1_file, self.temp_rawcooked_v2_file, self.temp_review_file]

        self.mkv_cooked_folder = os.path.join(MKV_DEST, "mkv_cooked/")

    def rawcooked_command_executor(self, start_folder_path: str, mkv_file_name: str, md5_checksum: bool = False,
                                   v2: bool = False):

        string_command = f"rawcooked --license 004B159A2BDB07331B8F2FDF4B2F -y --all --no-accept-gaps {'--output-version 2' if v2 else ''} -s 5281680 {'--framemd5' if md5_checksum else ''} {start_folder_path} -o {MKV_DEST}mkv_cooked/{mkv_file_name}.mkv"
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

        # If Rawcooked is executed using output v2 and a gap is found
        if v2 and std_logs.find('Warning: incoherent file names') != -1:
            logging_utils.log(self.logfile,
                              f"FAIL: {start_folder_path} CONTAINS INCOHERENT SEQUENCES. Adding to "
                              f"temp_review_dpx_list.txt")
            with open(self.temp_review_file, 'a') as file:
                file.write(f"{start_folder_path}\n")

    def process(self):
        if not os.path.exists(self.logfile):
            with open(self.logfile, 'w+'):
                pass

        if not os.path.exists(self.review_dpx_failure_log):
            with open(self.review_dpx_failure_log, 'w+'):
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
                    executor.submit(self.rawcooked_command_executor, seq_path, mkv_file_name, False, True)

                # Cooking with --framemd5 flag
                with concurrent.futures.ProcessPoolExecutor() as executor:
                    executor.submit(self.rawcooked_command_executor, seq_path, mkv_file_name, True, True)

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
                    executor.submit(self.rawcooked_command_executor, seq_path, mkv_file_name, False, False)

                # Cooking with --framemd5 flag
                with concurrent.futures.ProcessPoolExecutor() as executor:
                    executor.submit(self.rawcooked_command_executor, seq_path, mkv_file_name, True, False)

    def process_temporary_files(self) -> list:
        dpx_review_list = []
        with open(self.temp_review_file, 'r') as file:
            for line in file.readlines():
                dpx_review_list.append(line.strip())

        dpx_review_list = set(dpx_review_list)
        for line in dpx_review_list:
            with open(self.review_dpx_failure_log, 'a') as file:
                file.write(f"{line}\n")

        success_list = []
        with open(self.temp_rawcooked_v2_file, 'r') as file:
            for line in file.readlines():
                line = line.strip()
                if line not in dpx_review_list:
                    success_list.append(line)

        for line in success_list:
            with open(self.rawcooked_v2_success_log, 'a') as file:
                file.write(f"{line}\n")

        return list(dpx_review_list)

    def process_review_sequences(self, sequences_to_review: list):
        for seq in sequences_to_review:

            # Move the sequence folder to dpx_for_review
            if not os.path.exists(os.path.join(DPX_FOR_REVIEW_PATH, os.path.basename(seq))):
                shutil.move(seq, DPX_FOR_REVIEW_PATH)
                logging_utils.log(self.logfile, f"MOVED {seq} to dpx_for_review folder")
            else:
                logging_utils.log(self.logfile, f"CAN NOT MOVE {seq} to dpx_for_review folder. A sequence with same "
                                                f"name already exists")

            # Remove MD5 file
            md5_path = f"{seq}.framemd5"
            if os.path.exists(md5_path):
                os.remove(md5_path)
                logging_utils.log(self.logfile, f"DELETED: {md5_path}")

            # Remove the cooked .mkv and respective .txt files
            mkv_file_name = f"{os.path.basename(seq)}.mkv"
            txt_file_name = f"{mkv_file_name}.txt"
            mkv_file_path = os.path.join(self.mkv_cooked_folder, mkv_file_name)
            txt_file_path = os.path.join(self.mkv_cooked_folder, txt_file_name)
            if os.path.exists(mkv_file_path):
                os.remove(mkv_file_path)
                logging_utils.log(self.logfile, f"DELETED: {mkv_file_path}")
            if os.path.exists(txt_file_path):
                os.remove(txt_file_path)
                logging_utils.log(self.logfile, f"DELETED: {txt_file_path}")

    def clean(self):
        self.process_temporary_files()
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

        dpx_review_list = self.process_temporary_files()
        if len(dpx_review_list) > 0:
            self.process_review_sequences(dpx_review_list)

        self.clean()


if __name__ == '__main__':
    dpx_rawcook = DpxRawcook()
    dpx_rawcook.execute()
