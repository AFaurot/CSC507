import time


# streaming test
def add_files_streaming(input1, input2, output):
    with open(input1, 'r') as f1, open(input2, 'r') as f2, open(output, 'w') as out:
        for line1, line2 in zip(f1, f2):
            val1 = line1.strip()
            val2 = line2.strip()
            if val1 and val2:
                try:
                    sum_val = int(val1) + int(val2)
                    out.write(f"{sum_val}\n")
                except ValueError:
                    # Handle the case where a line isn't an integer
                    pass  # or log the error


def main():
    start = time.time()
    add_files_streaming("test_add1.txt", "test_add2.txt", "summation.txt")
    end = time.time()
    print(f"Processing took {end - start:.2f} seconds")


if __name__ == '__main__':
    main()
