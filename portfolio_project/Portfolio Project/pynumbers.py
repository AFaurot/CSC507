import random
import time

# simple script to generate a file - For the portfolio project it can be run twice
# Name one file hugefile1.txt and the other file hugefile2.txt
# Note that this may take around 5 minutes to generate 1 billion rows
file = open("../hugefile1.txt", "w")
start_time = time.time()
for i in range(1_000_000_000):
    random_number = random.randint(1,100000)
    file.write(str(random_number))
    file.write("\n")
file.close()
end_time = time.time()
execution_time = end_time-start_time
print(f"Execution took: {execution_time} seconds")