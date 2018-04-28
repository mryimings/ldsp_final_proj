
def get_media(line):
    line = line.strip().split(",")
    # print(line)
    if not line[0].isdigit() or len(line) < 3:
        return None
    if line[2][0] in ["'", '"']:
        symbol = line[2][0]
        index = 2
        while index < len(line) and line[index][-1] != symbol:
            index += 1
        index += 1
        if index >= len(line):
            return None
        else:
            return line[index]
    return line[3]

medias = set()
for i in range(1, 4):
    print("{}----------".format(i))
    with open("./all-the-news/article{}.csv".format(i), "r") as f:
        count = 0
        for line in f:
            if line[0].isdigit:
                try:
                    medias.add(get_media(line))
                except IndexError:
                    continue

print(medias)