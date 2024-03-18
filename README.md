<pre><font color="#3465A4"><b>.</b></font>
├── <font color="#3465A4"><b>logs</b></font>
│   ├── dpx_assessment.log
│   ├── dpx_post_rawcook.log
│   └── dpx_rawcook.log
├── <font color="#3465A4"><b>media</b></font>
│   └── <font color="#3465A4"><b>encoding</b></font>
│       ├── <font color="#3465A4"><b>dpx_for_review</b></font>
│       │   └── <font color="#3465A4"><b>post_rawcook_fails</b></font>
│       │       ├── <font color="#3465A4"><b>mkv_files</b></font>
│       │       └── <font color="#3465A4"><b>rawcook_output_logs</b></font>
│       ├── <font color="#3465A4"><b>dpx_to_assess</b></font>
│       └── <font color="#3465A4"><b>rawcooked</b></font>
│           ├── <font color="#3465A4"><b>dpx_to_cook</b></font>
│           ├── <font color="#3465A4"><b>dpx_to_cook_v2</b></font>
│           └── <font color="#3465A4"><b>encoded</b></font>
│               └── <font color="#3465A4"><b>mkv_cooked</b></font>
├── <font color="#3465A4"><b>policy</b></font>
└──.env 
</pre>
## Setup
To get started, please follow these steps to set up the necessary environment variables:

1. **Clone the Repository**: Start by cloning the project repository to your local machine using Git:

    ```bash
    git clone https://github.com/saiprasa-usc/rawcooked_usc-dr.git
    ```
2. **Rename `.env.example` to `.env`**: In the root directory of the cloned repository, you will find a file named `.env.example`. Rename this file to `.env`. This file will hold your environment variables.

    ```bash
    mv .env.example .env
    ```
3. **Fill in the Variables**: Open the `.env` file in a text editor of your choice. You will see a list of environment variables with placeholder values. Replace these placeholders with the actual values corresponding to your system environment variables and paths to different folders.

    Example:
    ```
    FILM_OPS=<path-to-project>
    DPX_SCRIPT_LOG=<path-to-logs-folder>
    ...
    ```
4. **Save the Changes**: Once you've filled in all the necessary variables, save the `.env` file.

## Workflow

Three Scripts:

- dpx_assessment.py
- dpx_rawcook.py
- dpx_post_rawcook.py

### dpx_assessment
- reads dpx_to_assess folder
- runs media conch policy checks on the sequences in this folder
- runs rawcooked --no-encode on the sequences to check for gaps or large reversibility files
- moves sequences with gaps to dpx_to_review >> dpx_with_gaps
- moves sequences with larger reversibility files to dpx_to_cook_v2
- moves the remaining sequences to dpx_to_cook

### dpx_rawcook.py
- runs rawcooked for sequences in dpx_to_cook folder and moves the mkvs to mkv_cooked folder
- runs rawcooked with output version 2 for sequences in dpx_to_cook_v2 folder and moves the mkvs to mkv_cooked_v2 folder
- moves failed files to dpx_to_review > rawcooked_failed or dpx_to_review > rawcooked_v2_failed

### dpx_post.py
- runs mediaconch policy checks on the mkv files in the cooked folders
- moves fails to mkx_to_review > mediaconch_fails
- moves successfully dpx sequences to dpx_completed folder
- check general errors, stalled encodings and incomplete cooks (TODO: decide folder structure)

## Logging
- three log files for each script
- overall log
- success log
- failure log
