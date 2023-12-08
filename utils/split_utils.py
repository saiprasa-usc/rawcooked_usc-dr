import os


# Prepare file for DPX splitting script/move to preservation
def sort_split_list(textfile):
    """Takes a text file containing DPX file paths and process them by sorting into a list. Also modifies the input file sorting the data

    @param textfile: The text file containing the list of all DPX file paths
    @return: list of file paths
    """
    split_list = []
    if os.path.isfile(textfile) and os.path.getsize(textfile) > 0:
        with open(textfile, 'r') as file:
            split_list = file.read().splitlines()
        split_list.sort()
        with open(textfile, 'w') as file:
            file.write("\n".join(split_list))
    return split_list


def create_python_list(source_textfile, target_textfile, type):
    size_dict = {}
    if os.path.isfile(source_textfile) and os.path.getsize(source_textfile) > 0:
        with open(target_textfile, 'a') as target:
            with open(source_textfile, 'r') as source:
                for line in source:
                    kb_size = os.popen(f"du -s {line}").read().strip().split()[0]
                    size_dict[line] = kb_size
                    target.write(kb_size + "% " + line + "% " + type)
    return size_dict
