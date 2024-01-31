<pre><span style="background-color:#4E9A06"><font color="#3465A4">media</font></span>
└── <span style="background-color:#4E9A06"><font color="#3465A4">encoding</font></span>
    ├── <font color="#3465A4"><b>dpx_for_review</b></font>
    ├── <font color="#3465A4"><b>dpx_to_assess</b></font>
    │   ├── rawcook_dpx_success.log
    │   ├── review_dpx_failures.log
    │   └── tar_dpx_failures.log
    └── <span style="background-color:#4E9A06"><font color="#3465A4">rawcooked</font></span>
        ├── <font color="#3465A4"><b>dpx_to_cook_v2</b></font>
        ├── <font color="#3465A4"><b>dpx_to_cook</b></font>
        └── <span style="background-color:#4E9A06"><font color="#3465A4">encoded</font></span>
            ├── <span style="background-color:#4E9A06"><font color="#3465A4">killed</font></span>
            ├── <span style="background-color:#4E9A06"><font color="#3465A4">logs</font></span>
            ├── <span style="background-color:#4E9A06"><font color="#3465A4">mkv_cooked</font></span>
            ├── rawcooked_success.log
            ├── reversibility_list.txt
            ├── temp_queued_list.txt
            └── temp_rawcooked_success.log
</pre>

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
