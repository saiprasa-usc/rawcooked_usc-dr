import concurrent
import subprocess
import os
from concurrent.futures import ThreadPoolExecutor
from shutil import move


def get_media_info(flags, filename, output_file=None):
    """Function to run the mediainfo command via shell and dump the output in a file

    @param flags: Mediainfo command flags
    @param filename: The path of the file over which the mediainfo command will execute
    @param output_file: The file where the output of the mediainfo will get stored
    @return: The decoded output of the mediainfo execution
    """

    command = ['mediainfo', flags, filename]
    media_info_check = subprocess.run(command, capture_output=True)
    print(" ".join(media_info_check.args))
    media_info = media_info_check.stdout.decode()
    if output_file:
        with open(output_file, 'w') as file:
            file.write(media_info)
    return media_info


def generate_tree(directory_path, indent='', output_file=None):
    """Function to create a directory tree structure and store it in a file

    @param directory_path: The root path
    @param indent: Indentation character
    @param output_file: Name of the output file
    """

    if not output_file:
        output_file = open(directory_path + '_directory_contents.txt', 'w')
    else:
        output_file = open(output_file, 'w')

    output_file.write(f"{indent}{os.path.basename(directory_path)}\n")
    indent += '|-- '

    for root, dirs, files in os.walk(directory_path):
        for directory in dirs:
            output_file.write(f"{indent}{directory}\n")
        for file in files:
            output_file.write(f"{indent}{file}\n")

    output_file.close()


def check_mediaconch_policy(policy_path, filename):
    """Function to execute the mediaconch command and return the results

    @param policy_path: Path to the file containing the policies
    @param filename: The DPX file path which is verified against the policies
    @return: The output of the mediaconch command
    """

    command = ['mediaconch', '--force', '-p', policy_path, filename]
    check = subprocess.run(command, capture_output=True)
    return check


def run_script(python_version, script_name, argument):
    subprocess.run([python_version, script_name, argument], check=True)


def run_command(command, argument=None):
    if argument:
        command = command.replace("argument", argument)
    print("Running: " + command)
    subprocess.run(command, shell=True, check=True)


def move_file(filename, source_dir, destination_dir):
    source_path = os.path.join(source_dir, filename)
    destination_path = os.path.join(destination_dir, filename)
    try:
        move(source_path, destination_path)
        print(f"Moved: {source_path} to {destination_path}")
    except Exception as e:
        print(f"Error moving {source_path} to {destination_path}: {e}")


def move_files_parallel(source_dir, destination_dir, file_list, num_jobs):
    with ThreadPoolExecutor(max_workers=num_jobs) as executor:
        futures = {executor.submit(move_file, filename, source_dir, destination_dir): filename for filename in
                   file_list}

        for future in concurrent.futures.as_completed(futures):
            filename = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"Error moving {filename}: {e}")


def create_file(file):
    if not os.path.exists(file):
        with open(file, "w+"):
            pass
