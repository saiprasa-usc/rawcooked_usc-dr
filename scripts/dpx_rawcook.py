import concurrent.futures
import itertools
import os
import shutil
import subprocess

from dotenv import load_dotenv

from utils import logging_utils, find_utils, shell_utils

load_dotenv()

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
                                   v2: bool = False) -> None:
        """The method passed to each process that executes rawcooked command

        Runs rawcooked command with respective parameters
        Stores the rawcooked console output to a .txt  file named as <mkv_file_name>.mkv.txt
        Checks if there are gaps in output v2 sequence, then that sequence is added to temp_review_list.txt
        """

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

    def process(self) -> None:
        """Initiates the workflow

        Creates log files if not present and creates temporary files
        """
        shell_utils.create_file(self.logfile)
        shell_utils.create_file(self.review_dpx_failure_log)
        shell_utils.create_file(self.rawcooked_v1_success_log)
        shell_utils.create_file(self.rawcooked_v2_success_log)

        # create the temporary files
        for file_name in self.file_names:
            shell_utils.create_file(file_name)

        # Write a START note to the logfile if files for encoding, else exit
        logging_utils.log(self.logfile, "============= DPX RAWcook script START =============")

    def pass_one(self) -> None:
        """Executes Rawcooked over the sequences present in dpx_to_cook_v2

        These sequences have large reversibility file and thus needs to be cooked with --output-version 2 flag
        Runs Rawcooked twice, once without the --framemd5 flag and then with --framemd5 flag
        The processed sequences are added to the temp_rawcooked_v2_list.txt file
        """

        # Run first pass where list generated for large reversibility cases by dpx_post_rawcook.sh
        logging_utils.log(self.logfile, "Checking for files that failed RAWcooked due to large reversibility files")

        # <sequence path, dpx_folder_path> pairs
        sequence_map = find_utils.find_dpx_folder_from_sequence(DPX_V2_PATH)

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

    def pass_two(self) -> None:
        """Executes Rawcooked over the sequences present in dpx_to_cook

        These sequences do NOT need to be cooked with --output-version 2 flag
        Runs Rawcooked twice, once without the --framemd5 flag and then with --framemd5 flag
        The processed sequences are added to the temp_rawcooked_v1_list.txt file
        """

        logging_utils.log(self.logfile, "Checking for files to cook using RAWCooked V1")
        sequence_map = find_utils.find_dpx_folder_from_sequence(DPX_PATH)

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
        """Process the data inside temporary files and returns a list of sequences that needs review

        Reads the sequence paths with gaps from temp_review_list.txt
        And stores them into review_dpx_failure.log
        Removes these failed files from temp_rawcooked_v2_list.txt
        And stores the successfully cooked files into rawcooked_dpx_v2_success.log

        Return the list of failed sequence paths
        """

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

    def process_review_sequences(self, sequences_to_review: list) -> None:
        """Process the failed sequences that need manual review

        Takes a list of sequence paths as input
        Moves these sequences to dpx_for_review folder
        Removes the .framemd5 file
        Removes the cooked .mkv and the respective .txt file from encoded/mkv_cooked folder
        """

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
        """Concludes the workflow

        Adds all the successfully cooked v1 files from temp_rawcooked_v1_list.txt to rawcooked_dpx_v1_success.log
        Removes all the temporary files created during the workflow
        """

        with open(self.rawcooked_v1_success_log, 'a') as target:
            with open(self.temp_rawcooked_v1_file, 'r') as source:
                target.write(source.read())

        # Clean up temporary files
        for file_name in self.file_names:
            if os.path.exists(file_name):
                os.remove(file_name)
                print(f"Deleted file: {file_name}")

        logging_utils.log(self.logfile, "============= DPX RAWcook script END =============")

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
