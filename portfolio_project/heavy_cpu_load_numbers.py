import time
import math


# add_cpu_load to add some computational overhead
def add_cpu_load(number):
    result = 0
    for i in range(10):
        result += math.ceil(math.sqrt(number) * math.log(number))
    return result


# Method to read entire file into memory before processing it
def read_all():

    with open("file2.txt", "r") as f:

        content = f.readlines()

    doubled = []
    for line in content:
        stripped = line.strip()
        if stripped:
            number = int(stripped)
            add_me = add_cpu_load(number)
            result = (number * 2) + add_me
            doubled.append(str(result))
    # Write file
    with open('output_method1.txt', 'w') as f:
        for value in doubled:
            f.write(value + '\n')


# Method to read file line by line
def line_by_line():

    # read file line by line
    with open('file2.txt', 'r') as infile, open('output_method2.txt', 'w') as outfile:
        for line in infile:
            stripped = line.strip()
            if stripped:
                number = int(stripped)
                add_me = add_cpu_load(number)
                result = (number * 2) + add_me
                outfile.write(str(result) + '\n')


# Method to split the file in twain first before processing each half
def split_file():

    #  Read original and split into two files
    with open('file2.txt', 'r') as original:
        lines = []
        for line in original:
            stripped = line.strip()
            if stripped:
                lines.append(stripped)

    midpoint = len(lines) // 2
    part1_lines = lines[:midpoint]
    part2_lines = lines[midpoint:]

    with open('file_part1.txt', 'w') as f1:
        for line in part1_lines:
            f1.write(line + '\n')

    with open('file_part2.txt', 'w') as f2:
        for line in part2_lines:
            f2.write(line + '\n')

    # Process both split files and write results
    doubled = []

    for part_file in ['file_part1.txt', 'file_part2.txt']:
        with open(part_file, 'r') as f:
            for line in f:
                stripped = line.strip()
                if stripped:
                    number = int(stripped)
                    add_me = add_cpu_load(number)
                    result = (number * 2) + add_me
                    doubled.append(str(result))

    with open('output_method3.txt', 'w') as f:
        for value in doubled:
            f.write(value + '\n')


def main():

    print("Running Method 1: Read entire file into memory")
    a_start_time = time.time()
    read_all()
    a_end_time = time.time()
    a_execution_time = a_end_time - a_start_time

    print("Running Method 2: Read file line-by-line")

    l_start_time = time.time()
    line_by_line()
    l_end_time = time.time()
    l_execution_time = l_end_time - l_start_time

    print("Running Method 3: Split file and process separately")

    s_start_time = time.time()
    split_file()
    s_end_time = time.time()
    s_execution_time = s_end_time - s_start_time

    print(f"Method 1 took {a_execution_time} seconds")
    print(f"Method 2 took {l_execution_time} seconds")
    print(f"Method 3 took {s_execution_time} seconds")


if __name__ == '__main__':
    main()

