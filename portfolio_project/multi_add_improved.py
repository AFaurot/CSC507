import os
import time
import math
import shutil
import platform
import subprocess
from multiprocessing import Pool, cpu_count, Process
from filesplit.split import Split

CHUNKS = 10  # Adjust as needed for partition size

FILE1 = 'hugefile1.txt'
FILE2 = 'hugefile2.txt'

SPLIT_DIR1 = 'splits1'
SPLIT_DIR2 = 'splits2'
OUTPUT_DIR = 'outputs'
FINAL_OUTPUT = 'totalfile2.txt'


def is_wsl_available():
    return shutil.which('wsl') is not None


# Example usage
def split_file_auto(file_path, output_dir):
    """
    Automatically splits a file:
    - On Windows: uses Python filesplit or WSL split.
    - On Unix: uses 'wc -l' to count lines, calculates lines per chunk using CHUNKS.
      Falls back to Python method if anything fails.

    Args:
        file_path (str): File to split.
        output_dir (str): Where to put split files.
    """
    if platform.system() == 'Windows':
        if is_wsl_available():
            print("Detected Windows and WSL— using WSL-based splitter.")
            try:
                # Use wc -l to count total lines
                result = subprocess.run(
                    ["wsl", "-e", "bash", "-c", f"wc -l < '{file_path}'"],
                    capture_output=True,
                    text=True,
                    check=True
                )
                total_lines = int(result.stdout.strip())

                # Compute lines per chunk using global CHUNKS
                lines_per_chunk = math.ceil(total_lines / CHUNKS)

                print(f"{file_path} has {total_lines} lines — splitting into chunks of ~{lines_per_chunk} lines.\n")
                split_file_with_wsl(file_path, output_dir, lines_per_chunk)

            except (subprocess.CalledProcessError, FileNotFoundError, ValueError) as e:
                print(f"Bash/wc error: {e}")
                print("Falling back to Python splitter...")
                total_lines = count_lines_python(file_path)
                lines_per_chunk = math.ceil(total_lines / CHUNKS)
                print(f"{file_path} has {total_lines} lines — splitting into chunks of ~{lines_per_chunk} lines.\n")
                split_file_by_fixed_lines(file_path, output_dir, lines_per_chunk)
        else:
            print("Detected Windows — using Python-based splitter.")
            print("Counting lines...")
            total_lines = count_lines_python(file_path)
            lines_per_chunk = math.ceil(total_lines / CHUNKS)
            print(f"{file_path} has {total_lines} lines — splitting into chunks of ~{lines_per_chunk} lines.\n")
            split_file_by_fixed_lines(file_path, output_dir, lines_per_chunk)
    else:
        print("Detected Unix — using 'wc -l' + Bash split.")
        try:
            # Use wc -l to count total lines
            result = subprocess.run(
                ["/bin/bash", "-c", f"wc -l < '{file_path}'"],
                capture_output=True,
                text=True,
                check=True
            )
            total_lines = int(result.stdout.strip())

            # Compute lines per chunk using global CHUNKS
            lines_per_chunk = math.ceil(total_lines / CHUNKS)

            print(f"{file_path} has {total_lines} lines — splitting into chunks of ~{lines_per_chunk} lines.\n")
            split_file_with_bash(file_path, output_dir, lines_per_chunk)

        except (subprocess.CalledProcessError, FileNotFoundError, ValueError) as e:
            print(f"Bash/wc error: {e}")
            print("Falling back to Python splitter...")
            total_lines = count_lines_python(file_path)
            lines_per_chunk = math.ceil(total_lines / CHUNKS)
            print(f"{file_path} has {total_lines} lines — splitting into chunks of ~{lines_per_chunk} lines.\n")
            split_file_by_fixed_lines(file_path, output_dir, lines_per_chunk)


# Count lines in file, used in Windows or if wc is not found
def count_lines_python(file_path):
    with open(file_path, 'r') as f:
        return sum(1 for line in f if line.strip())


def split_file_with_wsl(file_path, output_dir, lines_per_chunk):
    """
    Uses Bash's 'split' command inside WSL to split a file by a fixed number of lines,
    and renames output files to 'chunk_0.txt', 'chunk_1.txt', etc.

    Args:
        file_path (str): File to split.
        output_dir (str): Directory for output chunks.
        lines_per_chunk (int): Number of lines per chunk.
    """
    os.makedirs(output_dir, exist_ok=True)

    # Define prefix for split files (e.g., splits1/chunk_)
    prefix = f"{output_dir}/chunk_"

    # Compose the Bash command
    bash_command = f"split -l {lines_per_chunk} '{file_path}' '{prefix}'"

    try:
        subprocess.run(["wsl", "-e", "bash", "-c", bash_command], check=True)
    except subprocess.CalledProcessError as e:
        print(f"wsl split failed: {e}")
        return

    # Rename split output (chunk_aa → chunk_0.txt, etc.)
    files = sorted(os.listdir(output_dir))
    for idx, filename in enumerate(files):
        src = os.path.join(output_dir, filename)
        dst = os.path.join(output_dir, f"chunk_{idx}.txt")
        os.rename(src, dst)


def split_file_with_bash(file_path, output_dir, lines_per_chunk):
    """
    Uses Bash's 'split' command to split a file by a fixed number of lines,
    and renames output files to 'chunk_0.txt', 'chunk_1.txt', etc.

    Args:
        file_path (str): File to split.
        output_dir (str): Directory for output chunks.
        lines_per_chunk (int): Number of lines per chunk.
    """
    os.makedirs(output_dir, exist_ok=True)

    # Define prefix for split files (e.g., splits1/chunk_)
    prefix = os.path.join(output_dir, "chunk_")

    # Compose the Bash command
    bash_command = f"split -l {lines_per_chunk} '{file_path}' '{prefix}'"

    try:
        subprocess.run(["/bin/bash", "-c", bash_command], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Bash split failed: {e}")
        return

    # Rename split output (chunk_aa → chunk_0.txt, etc.)
    files = sorted(os.listdir(output_dir))
    for idx, filename in enumerate(files):
        src = os.path.join(output_dir, filename)
        dst = os.path.join(output_dir, f"chunk_{idx}.txt")
        os.rename(src, dst)


def split_file_by_fixed_lines(file_path, output_dir, lines_per_chunk):
    """
    Splits a file into chunks based on a fixed number of lines per chunk.

    Args:
        file_path (str): Path to the input file.
        output_dir (str): Directory to store the chunk files.
        lines_per_chunk (int): Number of lines per chunk.
    """
    os.makedirs(output_dir, exist_ok=True)

    # Split using filesplit
    splitter = Split(file_path, output_dir)
    splitter.bylinecount(lines_per_chunk)

    # Remove manifest file if it exists
    manifest_path = os.path.join(output_dir, "manifest")
    if os.path.exists(manifest_path):
        os.remove(manifest_path)

    # Rename files to chunk_0.txt, chunk_1.txt, ...
    files = sorted(os.listdir(output_dir))  # Sort for consistency
    for count, file in enumerate(files):
        src = os.path.join(output_dir, file)
        dst = os.path.join(output_dir, f"chunk_{count}.txt")
        os.rename(src, dst)


def clean_dirs():
    for d in [SPLIT_DIR1, SPLIT_DIR2, OUTPUT_DIR]:
        if os.path.exists(d):
            shutil.rmtree(d)
    # Uncomment below lines to remove the outputted file
    # if os.path.exists(FINAL_OUTPUT):
    #     os.remove(FINAL_OUTPUT)


def process_file_pair(index):
    """Process one chunk from each file, line-by-line add, write output."""
    file1 = os.path.join(SPLIT_DIR1, f"chunk_{index}.txt")
    file2 = os.path.join(SPLIT_DIR2, f"chunk_{index}.txt")
    output_file = os.path.join(OUTPUT_DIR, f"out_chunk_{index}.txt")

    with open(file1, 'r') as f1, open(file2, 'r') as f2, open(output_file, 'w') as out:
        for line1, line2 in zip(f1, f2):
            val1 = line1.strip()
            val2 = line2.strip()
            if val1 and val2:
                try:
                    result = int(val1) + int(val2)
                    out.write(f"{result}\n")
                except ValueError:
                    continue  # skip invalid lines

    return output_file


def combine_outputs(output_dir=OUTPUT_DIR, final_output=FINAL_OUTPUT):
    with open(final_output, 'w') as outfile:
        for i in range(CHUNKS):  # enforce chunk order explicitly
            output_file = os.path.join(output_dir, f"out_chunk_{i}.txt")
            with open(output_file, 'r') as f:
                shutil.copyfileobj(f, outfile)  # faster than readlines + writelines


def parallel_process_file_pairs():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    indices = list(range(CHUNKS))
    with Pool(processes=min(CHUNKS, cpu_count())) as pool:
        pool.map(process_file_pair, indices)


def main():
    clean_dirs()
    if is_wsl_available():
        print("WSL is available in PATH.")
    else:
        print("WSL not found in PATH.")

    start = time.time()
    print(f"Splitting input files into {CHUNKS} parts...")
    # Automatically choose best file splitting method depending on OS
    p1 = Process(target=split_file_auto, args=(FILE1, SPLIT_DIR1))
    p2 = Process(target=split_file_auto, args=(FILE2, SPLIT_DIR2))

    p1.start()
    p2.start()
    p1.join()
    p2.join()
    end = time.time()
    print(f"Splitting took {end - start:.2f} seconds")

    p_start = time.time()
    print("Processing file chunks in parallel...")
    parallel_process_file_pairs()
    p_end = time.time()
    print(f"Processing took {p_end - p_start:.2f} seconds")

    c_start = time.time()
    print("Combining output files into final result...")
    combine_outputs()
    final_end = time.time()
    print(f"Combining took {final_end - c_start:.2f} seconds")
    print(f"\nTime after splitting = {final_end - p_start:.2f} seconds")
    print(f"Done. Total operations took {final_end - start:.2f} seconds")
    clean_dirs()


if __name__ == '__main__':
    main()
