import os
import time
import math
import shutil
from multiprocessing import Pool, cpu_count

CHUNKS = 20  # Number of split files

INPUT_FILE = 'file2.txt'
SPLIT_DIR = 'splits'
OUTPUT_DIR = 'outputs'
FINAL_OUTPUT = 'output_parallel.txt'


# add_cpu_load to add some computational overhead
def add_cpu_load(number):
    result = 0
    for i in range(10):
        result += math.ceil(math.sqrt(number) * math.log(number))
    return result


# clean up temporary outputs after script completion
def clean_dirs():
    if os.path.exists(SPLIT_DIR):
        shutil.rmtree(SPLIT_DIR)
    if os.path.exists(OUTPUT_DIR):
        shutil.rmtree(OUTPUT_DIR)
    #if os.path.exists(FINAL_OUTPUT):
     #   os.remove(FINAL_OUTPUT)


def process_file(filename):
    input_path = os.path.join(SPLIT_DIR, filename)
    output_path = os.path.join(OUTPUT_DIR, f"out_{filename}")

    with open(input_path, 'r') as f, open(output_path, 'w') as out:
        for line in f:
            stripped = line.strip()
            if stripped:
                try:
                    number = int(stripped)
                    if number <= 0:
                        continue  # skip zero or negative values
                    add_me = add_cpu_load(number)
                    result = (number * 2) + add_me
                    out.write(str(result) + '\n')
                except Exception as e:
                    print(f"Error in {filename}: {e}")
    return output_path


def split_file(input_file=INPUT_FILE, chunks=CHUNKS):
    with open(input_file, 'r') as f:
        lines = [line for line in f if line.strip()]

    os.makedirs(SPLIT_DIR, exist_ok=True)
    total_lines = len(lines)
    chunk_size = (total_lines + chunks - 1) // chunks

    for i in range(chunks):
        chunk_lines = lines[i * chunk_size:(i + 1) * chunk_size]
        split_path = os.path.join(SPLIT_DIR, f"chunk_{i}.txt")
        with open(split_path, 'w') as chunk_file:
            chunk_file.writelines(chunk_lines)


def combine_outputs(output_dir=OUTPUT_DIR, final_output=FINAL_OUTPUT):
    with open(final_output, 'w') as outfile:
        for i in range(CHUNKS):
            output_file = os.path.join(output_dir, f"out_chunk_{i}.txt")
            if os.path.exists(output_file):
                with open(output_file, 'r') as f:
                    outfile.writelines(f.readlines())


def parallel_process_files():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    filenames = [f"chunk_{i}.txt" for i in range(CHUNKS)]

    with Pool(processes=min(CHUNKS, cpu_count())) as pool:
        pool.map(process_file, filenames)


def main():
    start = time.time()
    print(f"Splitting input file into {CHUNKS} partitions...")
    split_file()
    print("Processing split files in parallel...")
    parallel_process_files()
    print("Combining outputs into final file...")
    combine_outputs()
    print("Done.")
    end = time.time()
    print(f"Processing took {end - start:.2f} seconds")
    clean_dirs()


if __name__ == '__main__':
    main()
