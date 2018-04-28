import os
from nltk.tokenize import word_tokenize

def merge(wing):
    with open(os.path.join("./data", wing, wing+"-all.txt"), "w") as f:
        for filename in os.listdir(os.path.join("./data", wing)):
            with open(os.path.join('./data', wing, filename), "r") as file:
                for line in file:
                    f.write(' '.join(word_tokenize(line.strip())))
                    f.write("\n")

if __name__ == "__main__":
    merge("left")
    merge("right")