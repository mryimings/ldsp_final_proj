import sys
from collections import defaultdict
import math
import random
import json
import os
import os.path
from nltk.tokenize import word_tokenize



def corpus_reader(corpusfile, lexicon=None):
    with open(corpusfile, 'r') as corpus:
        for line in corpus:
            if line.strip():
                sequence = line.lower().strip().split()
                if lexicon:
                    yield [word if word in lexicon else "UNK" for word in sequence]
                else:
                    yield sequence


# do not modify this function
def get_lexicon(corpus):
    word_counts = defaultdict(int)
    for sentence in corpus:
        for word in sentence:
            word_counts[word] += 1
    return set(word for word in word_counts if word_counts[word] > 1)


def get_ngrams(sequence, n):
    """
    COMPLETE THIS FUNCTION (PART 1)
    Given a sequence, this function should return a list of n-grams, where each n-gram is a Python tuple.
    This should work for arbitrary values of 1 <= n < len(sequence).
    """
    ans = []
    length = len(sequence)
    for i in range(-1, length + 1):
        temp = []
        for j in range(i - n + 1, i + 1):
            if j < 0:
                temp.append('START')
            elif j >= length:
                temp.append('STOP')
            else:
                temp.append(sequence[j])
        ans.append(tuple(temp))

    return ans


class TrigramModel(object):

    def __init__(self, corpusfile=None, is_restore=False):

        # Iterate through the corpus once to build a lexicon
        if not is_restore:
            generator = corpus_reader(corpusfile)
            self.lexicon = get_lexicon(generator)
            self.lexicon.add("UNK")
            self.lexicon.add("START")
            self.lexicon.add("STOP")

            # Now iterate through the corpus again and count ngrams
            generator = corpus_reader(corpusfile, self.lexicon)
            self.count_ngrams(generator)

        else:
            self.restore(filename=corpusfile)

    def count_ngrams(self, corpus):
        """
        COMPLETE THIS METHOD (PART 2)
        Given a corpus iterator, populate dictionaries of unigram, bigram,
        and trigram counts.
        """

        self.unigramcounts = {}  # might want to use defaultdict or Counter instead
        self.bigramcounts = {}
        self.trigramcounts = {}

        self.word_count = 0.0

        dic_list = [self.unigramcounts, self.bigramcounts, self.trigramcounts]

        for line in corpus:
            for i, dic in enumerate(dic_list):
                ngram_seq = get_ngrams(line, i + 1)
                for gram in ngram_seq:
                    dic[gram] = dic.get(gram, 0) + 1
                    if i == 0:
                        self.word_count += 1

        return

    def raw_trigram_probability(self, trigram):
        """
        COMPLETE THIS METHOD (PART 3)
        Returns the raw (unsmoothed) trigram probability
        """
        return self.trigramcounts.get(trigram, 0) / self.bigramcounts.get((trigram[0], trigram[1]), 1)

    def raw_bigram_probability(self, bigram):
        """
        COMPLETE THIS METHOD (PART 3)
        Returns the raw (unsmoothed) bigram probability
        """
        return self.bigramcounts.get(bigram, 0) / self.unigramcounts.get((bigram[0],), 1)

    def raw_unigram_probability(self, unigram):
        """
        COMPLETE THIS METHOD (PART 3)
        Returns the raw (unsmoothed) unigram probability.
        """

        return self.unigramcounts.get(unigram, 0) / self.word_count

    # def generate_sentence(self,t=20):
    #     """
    #     COMPLETE THIS METHOD (OPTIONAL)
    #     Generate a random sentence from the trigram model. t specifies the
    #     max length, but the sentence may be shorter if STOP is reached.
    #     """
    #     return result

    def smoothed_trigram_probability(self, trigram):
        """
        COMPLETE THIS METHOD (PART 4)
        Returns the smoothed trigram probability (using linear interpolation).
        """
        lambda1 = 1 / 3.0
        lambda2 = 1 / 3.0
        lambda3 = 1 / 3.0
        return lambda1 * self.raw_unigram_probability((trigram[2],)) + \
               lambda2 * self.raw_bigram_probability((trigram[1], trigram[2])) + \
               lambda3 * self.raw_trigram_probability(trigram)

    def sentence_logprob(self, sentence):
        """
        COMPLETE THIS METHOD (PART 5)
        Returns the log probability of an entire sequence.
        """
        # ans = 0.0
        # for trigram in get_ngrams(sentence, 3)[1:]:
        #     ans += math.log2(self.smoothed_trigram_probability())
        return sum(math.log2(self.smoothed_trigram_probability(trigram)) for trigram in get_ngrams(sentence, 3)[1:])

    def perplexity(self, corpus):
        """
        COMPLETE THIS METHOD (PART 6)
        Returns the log probability of an entire sequence.
        """

        ans = 0.0
        word_num = 0
        for line in corpus:
            ans -= self.sentence_logprob(line)
            word_num += len(line)

        return 2 ** (ans / word_num)

    def save(self, path='./models', filename="model.txt"):
        with open(os.path.join(path, filename), 'w') as f:
            f.write(json.dumps({
                "lexicon": [x for x in self.lexicon],
                "unigramcounts": [x for x in self.unigramcounts.items()],
                "bigramcounts": [x for x in self.bigramcounts.items()],
                "trigramcounts": [x for x in self.trigramcounts.items()],
                "word_count": self.word_count
            }))

    def restore(self, path='./models', filename='model.txt'):
        with open (os.path.join(path, filename), "r") as f:
            for line in f:
                json_obj = json.loads(line.strip())
                self.lexicon = set(json_obj["lexicon"])

                self.unigramcounts = {}
                for item in json_obj["unigramcounts"]:
                    self.unigramcounts[tuple(item[0])] = item[1]

                self.bigramcounts = {}
                for item in json_obj["bigramcounts"]:
                    self.bigramcounts[tuple(item[0])] = item[1]

                self.trigramcounts = {}
                for item in json_obj["trigramcounts"]:
                    self.trigramcounts[tuple(item[0])] = item[1]

                self.word_count = json_obj["word_count"]

    def line_perplexity(self, line):
        # line = word_tokenize(line)
        line = [word.lower() if word in self.lexicon else "UNK" for word in line]
        return 2 ** ((-self.sentence_logprob(line)) / len(line))





def essay_scoring_experiment(training_file1, training_file2, testdir1, testdir2):
    model1 = TrigramModel(training_file1)
    model2 = TrigramModel(training_file2)

    total = 0
    correct = 0

    # for f in os.listdir(testdir1):
    pp1 = model1.perplexity(corpus_reader(os.path.join(testdir1, f), model1.lexicon))
    pp2 = model2.perplexity(corpus_reader(os.path.join(testdir1, f), model2.lexicon))
    if pp1 <= pp2:
        correct += 1
    total += 1

    for f in os.listdir(testdir2):
        pp1 = model1.perplexity(corpus_reader(os.path.join(testdir2, f), model1.lexicon))
        pp2 = model2.perplexity(corpus_reader(os.path.join(testdir2, f), model2.lexicon))
        if pp1 >= pp2:
            correct += 1
        total += 1

    return float(correct) / total


if __name__ == "__main__":
    train_file_left = './data/left/left-all.txt'
    train_file_right = './data/right/right-all.txt'
    left_model = TrigramModel(train_file_left)
    right_model = TrigramModel(train_file_right)
    left_model.save(filename="left-model.txt")
    right_model.save(filename="right-model.txt")

