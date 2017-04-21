__author__ = 'sunary'


from utils import my_text


class ReadPdf():

    def __init__(self):
        pass

    def read(self, pdf_file, output_csv):
        text_from_pdf = my_text.get_pdf_content(pdf_file)
        text_from_pdf = text_from_pdf.split('\n')[:-2]

        for i in range(len(text_from_pdf)):
            text_from_pdf[i] = text_from_pdf[i].split(' ')
            for j in range(len(text_from_pdf[i])):
                if text_from_pdf[i][j] == 'Ticker':
                    text_from_pdf[i] = text_from_pdf[i][j + 1:]
                    break

            len_list = len(text_from_pdf[i])
            j = 0
            while j < len_list:
                if text_from_pdf[i][j] == 'Company' or text_from_pdf[i][j] == 'Ticker':
                    del text_from_pdf[i][j]
                    len_list -= 1
                j += 1

        text_from_pdf = filter(lambda x: x, text_from_pdf)
        text_from_pdf = reduce(lambda x, y: x + y, text_from_pdf)
        text_from_pdf = my_text.find_by_index(text_from_pdf, cannot_first_last=set(['inc', 'in', 'corporation', 'corp', 'cor', 'co', 'ltd', 'tech', 'group', 'sa']))

        fo = open(output_csv, 'w')
        for text in text_from_pdf:
            fo.write(' '.join(text[:-1]).encode('utf-8') + ',' + text[-1].encode('utf-8') + '\n')
        fo.close()

if __name__ == '__main__':
    read_pdf = ReadPdf()
    read_pdf.read('/home/nhat/data/membership-russell-2000.pdf', '/home/nhat/data/from_pdf.csv')
