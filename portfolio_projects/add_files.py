import numpy as np


# Function to return numpy array after reading file
def read_numbers_from_file(filename):
    with open(filename, "r") as f:
        return np.array([int(line.strip()) for line in f if line.strip()])


# Function to add two arrays together
def add_files(file1, file2):
    array_1 = read_numbers_from_file(file1)
    array_2 = read_numbers_from_file(file2)
    return array_1 + array_2


def main():

    result = add_files("hugefile1.txt", "hugefile2.txt")
    # numpy for the win! Writing the file is much easier this way
    np.savetxt("totalfile.txt", result, fmt="%d")


if __name__ == '__main__':
    main()


