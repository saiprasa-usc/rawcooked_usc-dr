import subprocess
import os


def get_media_info(flags, filename, output_file=None):
    command = ['mediainfo', flags, filename]
    media_info_check = subprocess.run(command, capture_output=True)
    print(" ".join(media_info_check.args))
    media_info = media_info_check.stdout.decode()
    if output_file:
        with open(output_file, 'w') as file:
            file.write(media_info)
    return media_info


def generate_tree(directory_path, indent='', output_file=None):
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
