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
                    | "Get rid of punctuation" >> beam.Map(remove_punctuation)  # apply to each element/row in the file
                    | "Replace with single spaces" >> beam.Map(remove_multiple_spaces)
                    | "Get each word in a different row" >> beam.FlatMap(lambda line: line.split())
                    | "To enable counting later" >> beam.Map(lambda word: (word, 1))
                    | "Word-wise count" >> beam.CombinePerKey(sum)
                    | "Get output file generated" >> beam.io.WriteToText('word-wise-count')
                    # | beam.Map(print)
                   )

p1.run()