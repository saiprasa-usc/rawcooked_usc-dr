import re
import os


def iterate_folders(path):
    """
       Iterate supplied path and return list
       of filenames
    """
    file_nums = []
    filenames = []
    for root, _, files in os.walk(path):
        for file in files:
            if file.endswith(('.dpx', '.DPX')):
                file_nums.append((int(re.search(r'\d+\.dpx', file).group().split('.dpx')[0])))
                filenames.append(os.path.join(root, file))
    return file_nums, filenames


def find_missing(path):
    gaps = False
    file_nums, filenames = iterate_folders(path)
    file_range = [x for x in range(min(file_nums), max(file_nums) + 1)]
    first_dpx = filenames[file_nums.index(min(file_nums))]
    last_dpx = filenames[file_nums.index(max(file_nums))]
    missing = list(set(file_nums) ^ set(file_range))
    if len(missing) > 0:
        gaps = True
    return gaps
