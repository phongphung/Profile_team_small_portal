__author__ = 'sunary'


class BlacklistChecker():
    checker_token = []
    black_token = []
    def __init__(self):
        pass

    def add_tokens(self, token):
        if self.black_token:
            position = self._find(token)
            if token != self.black_token[position]:
                self.black_token[position + 1:position + 1] = [token]
        else:
            self.black_token.append(token)
        pass

    def _find(self, token):
        if not token:
            return 0

        left_position = 0
        right_position = len(self.black_token) - 1

        mid_position= (left_position + right_position)/2
        mid_value = self.black_token[mid_position]
        while left_position <= right_position:
            if token < mid_value:
                right_position = mid_position - 1
            else:
                left_position = mid_position + 1

            mid_position = (left_position + right_position)/2
            mid_value = self.black_token[mid_position]

        return left_position - 1

    def check(self, checker):
        self.checker_token = checker

        for i in range(len(self.checker_token)):
            len_token = 1
            while True:
                list_token = self.checker_token[i: i + len_token]
                position = self._find(list_token) + 1

                if self.black_token[position - 1] == list_token:
                    # del self.black_token[position - 1]
                    return True

                if position >= len(self.black_token) or len_token > len(self.black_token[position]) or len_token > len(list_token) or\
                                self.black_token[position][len_token - 1] != list_token[len_token - 1]:
                    break
                len_token += 1
        return False

if __name__ == '__main__':
    check_token = BlacklistChecker()
    check_token.add_tokens([1, 2])
    check_token.add_tokens([5, 2])
    check_token.add_tokens([3, 4, 1])
    check_token.add_tokens([3, 4])
    check_token.add_tokens([2, 2])
    print check_token.black_token
    print check_token.check([1, 2, 3, 2, 2, 4, 45, 46, 4, 45, 52, 1, 21, 4, 5, 3, 4, 5, 1, 2])
