import os
import random
import sys
import time

from random_words import RandomWords
from random_words import LoremIpsum
from multiprocessing import Process
import multiprocessing as mp

output_dir = "/Users/chase/Downloads/wordcount_dataset_10GB/"
file_num = 100   # 生成10个文件
each_file_size = 100*1024*1024   # 单位 B

# # 产生单词
# rw = RandomWords()
# words = rw.random_words(count=10)
# print(words)

# each_file_size *= 1024
# # 产生句子
# for fileID in range(file_num):
#     li = LoremIpsum()
#     file_path = output_dir + "input_" + str(fileID)
#     with open(file_path, "w") as f:
#         written_size = 0
#         while written_size < each_file_size:
#             written_size += f.write(li.get_sentence() + "\n")
#         f.flush()


def generate_one_file(fileID):
    rw = RandomWords()
    li = LoremIpsum()
    file_path = output_dir + "input_" + str(fileID) + ".txt"
    with open(file_path, "w") as f:
        print("Generate " + file_path + ", ing")
        written_size = 0
        while written_size < each_file_size * 0.999:
            if random.random() < 0.96:
                written_size += f.write(li.get_sentence() + "\n")
            else:
                random_count = random.randint(1, 20)
                for word_a, word_b in zip(rw.random_words(count=random_count), rw.random_words(count=random_count)):
                    written_size += f.write(word_a + word_b + " ")
                written_size += f.write("\n")
        f.flush()

def run_process():
    process = []
    thread_num = 10
    for loop in range(file_num//thread_num):
        process.clear()
        for fileID in range(thread_num):
            process.append(mp.Process(target=generate_one_file, args=(loop * thread_num + fileID,)))
        [p.start() for p in process]
        [p.join() for p in process]
        # sys.stdout.flush()
        time.sleep(0.5 * each_file_size//1024//1024)
        # x = input()

if __name__ == '__main__':
    run_process()