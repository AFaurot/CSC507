import numpy as np


def read_chunk(file, chunk_size):
    """Read up to chunk_size lines, convert to ints, return NumPy array."""
    lines = []
    while len(lines) < chunk_size:
        line = file.readline()
        if not line:
            break
        stripped = line.strip()
        if stripped:
            try:
                lines.append(int(stripped))
            except ValueError:
                continue  # skip invalid lines
    return np.array(lines, dtype=int)


def add_files_numpy_chunks(file1_path, file2_path, output_path, chunk_size=100000):
    with open(file1_path, 'r') as f1, open(file2_path, 'r') as f2, open(output_path, 'w') as out:
        while True:
            chunk1 = read_chunk(f1, chunk_size)
            chunk2 = read_chunk(f2, chunk_size)

            if chunk1.size == 0 and chunk2.size == 0:
                break  # both files ended

            min_len = min(len(chunk1), len(chunk2))
            if min_len == 0:
                # One file is longer than the other, stop or handle accordingly
                break

            result = chunk1[:min_len] + chunk2[:min_len]

            # Write using NumPy's savetxt for speed & formatting
            np.savetxt(out, result, fmt='%d')


def main():
    add_files_numpy_chunks('test_add1.txt', 'test_add2.txt', 'summation.txt', chunk_size=100000)


if __name__ == '__main__':
    main()