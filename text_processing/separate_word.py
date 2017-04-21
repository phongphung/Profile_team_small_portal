__author__ = 'sunary'


import pandas as pd


class SeparateWord():

    separate_words = ['s', 'en', 'n']
    def __init__(self):
        self.get_dictionary()

    def get_dictionary(self):
        pd_file = pd.read_csv('dict.csv')
        self.list_words = pd_file['word']

    def find(self, word):
        if '-' in word:
            self.format_A_B(word)
        elif 'en' in word and self.format_AsB(word):
            pass
        else:
            self.format_AB(word)
        pass

    def format_A_B(self, word):
        split_word = word.split('-')
        if self.check_exist_dict(split_word[1]):
            if self.check_exist_dict(split_word[0]):
                print split_word[0] + ' ' + split_word[1]
                return True
            elif split_word[0][-1] == 's' and self.check_exist_dict(split_word[0][:-1]):
                print split_word[0][:-1] + ' ' + split_word[1]
                return True
        return False

    def format_AsB(self, word):
        id_separate = 1
        for i in range(1, len(word) - len(self.separate_words[id_separate])):
            if word[i:i + len(self.separate_words[id_separate])] == self.separate_words[id_separate] and self.check_exist_dict(word[:i]) and self.check_exist_dict(word[i + len(self.separate_words[id_separate]):]):
                print word[:i] + ' ' + word[i + len(self.separate_words[id_separate]):]
                return True
        return False

    def format_AB(self, word):
        word_position = self.get_position(word)
        while word[0] == self.list_words[word_position][0] and word_position > 0:
            word_position -= 1
            if word.startswith(self.list_words[word_position]) and self.check_exist_dict(word[len(self.list_words[word_position]):]):
                print self.list_words[word_position] + ' ' + word[len(self.list_words[word_position]):]
                return True
        return False

    def get_position(self, word):
        if not word:
            return 1

        left_position = 0
        right_position = len(self.list_words) - 1

        mid_position= (left_position + right_position)/2
        mid_word = self.list_words[mid_position]
        while left_position <= right_position:
            if word < mid_word:
                right_position = mid_position - 1
            else:
                left_position = mid_position + 1

            mid_position = (left_position + right_position)/2
            mid_word = self.list_words[mid_position]

        return left_position

    def check_exist_dict(self, word):
        return  self.list_words[self.get_position(word) - 1] == word

if __name__ == '__main__':
    check_word_combine = SeparateWord()
    check_word_combine.find('ballshoot')
    check_word_combine.find('mushroom')
    check_word_combine.find('aenbout')
    check_word_combine.find('asss-bout')
