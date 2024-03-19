import os
import logging
import re
import shutil
import sys
from utils import find_utils

from dotenv import load_dotenv

load_dotenv()

# Global variables extracted from environmental variables
SCRIPT_LOG = os.path.join(os.environ.get('FILM_OPS'), os.environ.get('DPX_SCRIPT_LOG'))
DPX_PATH = os.environ['FILM_OPS']
DPX_GAP_CHECK_PATH = os.path.join(DPX_PATH, os.environ['DPX_GAP_CHECK'])
DPX_REVIEW_PATH = os.path.join(DPX_PATH, os.environ['DPX_REVIEW'])
DPX_TO_ASSESS_PATH = os.path.join(DPX_PATH, os.environ['DPX_ASSESS'])

SCRIPT_LOG = r'{}'.format(SCRIPT_LOG)
DPX_PATH = r'{}'.format(DPX_PATH)
DPX_GAP_CHECK_PATH = r'{}'.format(DPX_GAP_CHECK_PATH)
DPX_REVIEW_PATH = r'{}'.format(DPX_REVIEW_PATH)
DPX_TO_ASSESS_PATH = r'{}'.format(DPX_TO_ASSESS_PATH)





class DpxGapCheck:

    def __init__(self):
        self.logfile = os.path.join(SCRIPT_LOG, 'dpx_gap_check.log')

    def iterate_folders(self, path):
        """
           Iterate suppied path and return list
           of filenames
        """
        file_nums = []
        filenames = []
        for root, _, files in os.walk(path):
            for file in files:
                if file.endswith(('.dpx', '.DPX')):
                    file_nums.append((int(re.search(r'\d+', file).group())))
                    filenames.append(os.path.join(root, file))
        return file_nums, filenames

    def find_missing(self, path):
        gaps = False
        file_nums, filenames = self.iterate_folders(path)
        file_range = [x for x in range(min(file_nums), max(file_nums) + 1)]
        first_dpx = filenames[file_nums.index(min(file_nums))]
        last_dpx = filenames[file_nums.index(max(file_nums))]
        missing = list(set(file_nums) ^ set(file_range))
        print(missing)
        if len(missing) > 0:
            gaps = True
        return gaps

    def execute(self):
        """
                Iterate all folders in dpx_gap_check/
                Check in each folder if DPX list is shorter than min() max() range list
                If yes, report different to log and move folder to dpx_for_review
                If identical, move folder to dpx_to_assess/ folder for folder type
                old folder formatting or new folder formatting
        """
        paths = [x for x in os.listdir(DPX_GAP_CHECK_PATH) if os.path.isdir(os.path.join(DPX_GAP_CHECK_PATH, x))]
        if not paths:
            sys.exit()
        sequence_map = find_utils.find_dpx_folder_from_sequence(DPX_GAP_CHECK_PATH)
        print(sequence_map)

        for dpath in sequence_map.values():
            has_gaps = self.find_missing(dpath)
            folder_name = dpath.replace(DPX_GAP_CHECK_PATH, '')
            if has_gaps:
                move_path = os.path.join(DPX_REVIEW_PATH, folder_name)
            else:
                move_path = os.path.join(DPX_TO_ASSESS_PATH, folder_name)

            try:
                shutil.move(dpath, move_path)
            except Exception as e:
                print(e)


if __name__ == '__main__':
    #TODO: Add Logging
    #TODO: Add Error Handling
    dpx_gap_check = DpxGapCheck()
    dpx_gap_check.execute()
