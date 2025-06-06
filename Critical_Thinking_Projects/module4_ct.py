# Algorithm to take process size and allocate it to first possible fit
def first_fit(block_sizes, process_sizes):

    # Initialize allocation list, setting to -1 (-1 designates not allocated)
    allocation_list = [-1] * len(process_sizes)

    # Outer loop for each process that needs memory (pid is index)
    # Inner loop goes over each block (bid is index)
    for pid, process in enumerate(process_sizes):
        # allocation  list is used to track which process get assigned to which index, returns index + 1.
        # process will assign first time the check evaluates to true
        for bid, block in enumerate(block_sizes):
            if block >= process:
                allocation_list[pid] = bid + 1
                # reduce memory in the block
                block_sizes[bid] -= process
                break
    return allocation_list


# Main function
def main():

    block_sizes = [100, 200, 400, 300, 600]
    process_sizes = [112, 242, 50, 312, 160, 140]

    print("----CSC 507: First Fit Simulation----")
    print("\nAvailable blocks", block_sizes)
    print("Process sizes", process_sizes)

    allocation = first_fit(block_sizes, process_sizes)

    # Display allocation in neat format
    print("\nProcess No.\tProcess Size\tBlock Allocated")
    for i, block in enumerate(allocation):
        if block != -1:
            print(f"{i + 1}\t\t\t\t{process_sizes[i]}\t\t\t{block}")
        else:
            print(f"{i + 1}\t\t\t\t{process_sizes[i]}\t\t\tNot Allocated")
    print("\nAvailable blocks after allocation", block_sizes)

# Press the green button in the gutter to run the script.


if __name__ == '__main__': main()

