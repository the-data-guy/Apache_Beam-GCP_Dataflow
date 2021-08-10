import apache_beam as beam


def split_row(element):
    return element.split(',')


def add_points_columns(element):  # penalty points
    element.append(0)  # default value for 10th element (short_payments)
    return element


def short_payments(element):  # condition 1
    if int(element[8]) < (int(element[6]) * 0.7):  # cleared_amount < 70% of monthly spend
        element[10] += 1  # 1 point for each penalty
    return element


def max_limit_but_not_cleared(element):  # condition 2
    if (int(element[6]) == int(element[5])) & (int(element[8]) < int(element[6])):
        element[10] += 1  # 1 point for each penalty
    return element


def additional_point(element):  # condition 3
    if element[10] == 2:
        element[10] += 1  # 1 extra penalty point for violating both conditions above
    return element


def filtering(record):
    return record[10] == 3


def get_select_elements(record):
    selected_elements = (record[0], record[10])  # convert to tuple, in order to do GroupBy later
    return selected_elements


class Counting (beam.DoFn):

    def process(self, element):
        (key, values) = element
        return [(key, sum(values))]


p1 = beam.Pipeline()

card_defaulters = (
                    p1
                    | beam.io.ReadFromText('cards.txt',  # modified 1st row to fulfil condition 2
                                           skip_header_lines=1)
                    | beam.Map(split_row)
                    | beam.Map(add_points_columns)
                    | beam.Map(short_payments)
                    | beam.Map(max_limit_but_not_cleared)
                    | beam.Map(additional_point)
                    # | beam.Filter(filtering)  # to check
                    | beam.Map(get_select_elements)  # ('CT30188', 1), ('CT30188', 0)
                    # | beam.Map(print)
                    | beam.GroupByKey()  # ('CT30188', [0, 1, 1, 0, 0, 1, 0, 0, 0, 1, 0, 0])
                    | beam.ParDo(Counting())  # ('CT30188', 4)
                    | beam.Map(print)
                   )

p1.run()
