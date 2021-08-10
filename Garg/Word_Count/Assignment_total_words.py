import apache_beam as beam


def remove_punctuation(row_string):
    punc = '''!()-[]{};:'"\,<>./?@#$%^&*_~'''
    for ele in row_string:
        if ele in punc:
            row_string = row_string.replace(ele, "")
    return row_string


def remove_multiple_spaces(row_string):
    return " ".join(row_string.split())


p1 = beam.Pipeline()

attendance_count = (
                    p1
                    | "Read from txt file" >> beam.io.ReadFromText('data.txt')
                    | beam.Map(remove_punctuation)  # apply to each row (element) in the file
                    | beam.Map(remove_multiple_spaces)  # replace with single spaces
                    | beam.Map(lambda row_text: row_text.split())  # get list of words in each row
                    | beam.Map(lambda row_words_list: ("Total no. of words: ", len(row_words_list)))
                    | beam.CombinePerKey(sum)  # word-count for ALL rows
                    | beam.Map(print)
                   )

p1.run()


