import multiprocessing
import time
import math
from multiprocessing import Pool, cpu_count

CHUNKS = 20  # You can change this to 2, 5, 20 etc.


# add_cpu_load to add some computational overhead
def add_cpu_load(number):
    result = 0
    for i in range(10):
        result += math.ceil(math.sqrt(number) * math.log(number))

    return result


# Computational processing for each chunk
def process_chunk(chunk_lines):

    result = []
    for line in chunk_lines:
        stripped = line.strip()
        if stripped:
            number = int(stripped)
            add_me = add_cpu_load(number)
            result.append(str((number * 2) + add_me))
    return result


def parallel_process_file(input_file='file2.txt', output_file='output_parallel2.txt', chunks=CHUNKS):
    # Read all lines and split into chunks
    with open(input_file, 'r') as f:
        lines = [line for line in f if line.strip()]

    total_lines = len(lines)
    chunk_size = (total_lines + chunks - 1) // chunks  # Ceiling division

    line_chunks = [lines[i:i + chunk_size] for i in range(0, total_lines, chunk_size)]

    # Use multiprocessing Pool to process chunks in parallel
    with Pool(processes=min(chunks, cpu_count())) as pool:
        results = pool.map(process_chunk, line_chunks)

    # Flatten results and write to output
    with open(output_file, 'w') as out:
        for chunk_result in results:
            for item in chunk_result:
                out.write(item + '\n')


def main():

    print(f"{CHUNKS} processes assigned a portion of the file ...")
    start = time.time()
    parallel_process_file()
    end = time.time()
    print(f"Completed in {end - start:.2f} seconds")


if __name__ == '__main__':
    main()
