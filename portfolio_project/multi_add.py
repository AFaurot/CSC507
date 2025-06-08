import os
import time
import shutil
from multiprocessing import Pool, cpu_count

CHUNKS = 10  # Adjusted for 4-core VM

FILE1 = 'hugefile1.txt'
FILE2 = 'hugefile2.txt'

SPLIT_DIR1 = 'splits1'
SPLIT_DIR2 = 'splits2'
OUTPUT_DIR = 'outputs'
FINAL_OUTPUT = 'totalfile2.txt'


def clean_dirs():
    for d in [SPLIT_DIR1, SPLIT_DIR2, OUTPUT_DIR]:
        if os.path.exists(d):
            shutil.rmtree(d)
    # if os.path.exists(FINAL_OUTPUT):
    #     os.remove(FINAL_OUTPUT)


def split_file_streaming(file_path, output_dir, chunks):
    os.makedirs(output_dir, exist_ok=True)

    # First count lines to determine chunk size
    with open(file_path, 'r') as f:
        total_lines = sum(1 for line in f if line.strip())

    lines_per_chunk = (total_lines + chunks - 1) // chunks  # ceiling division

    # Now split the file sequentially
    with open(file_path, 'r') as f:
        for chunk_index in range(chunks):
            output_path = os.path.join(output_dir, f"chunk_{chunk_index}.txt")
            with open(output_path, 'w') as chunk_file:
                written = 0
                while written < lines_per_chunk:
                    line = f.readline()
                    if not line:
                        break
                    if line.strip():
                        chunk_file.write(line)
                        written += 1


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
    start = time.time()
    print(f"Splitting input files into {CHUNKS} parts...")
    split_file_streaming(FILE1, SPLIT_DIR1, CHUNKS)
    split_file_streaming(FILE2, SPLIT_DIR2, CHUNKS)
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
    print(f"✅ Done. Total operations took {final_end - start:.2f} seconds")
    clean_dirs()


if __name__ == '__main__':
    main()
