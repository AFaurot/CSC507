import random
import time

file = open("file2.txt", "w")
start_time = time.time()
for i in range(10000000):
    random_number = random.randint(1,100000)
    file.write(str(random_number))
    file.write("\n")
file.close()
end_time = time.time()
execution_time = end_time-start_time
print(f"Execution took: {execution_time} seconds")