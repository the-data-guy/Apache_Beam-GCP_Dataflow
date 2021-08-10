import apache_beam as beam


def split_row(element):
    return element.split(',')


def remove_spaces(element):  # Note: Some columns values start with a space
    element[6] = element[6].replace(" ", "")
    element[8] = element[8].replace(" ", "")
    return element


def filter_medical_loans(element):
    return element[5] == "Medical Loan"


def add_points_column(element):  # penalty points
    element.append(0)  # default value for no. of late_payments
    return element


def late_payments(element):
    if element[8] > element[6]:
        element[9] += 1  # 1 point for each penalty
    return element


def get_select_elements(record):
    selected_elements = (record[0], record[9])  # convert to tuple, in order to do GroupBy later
    return selected_elements


class Counting(beam.DoFn):

    def process(self, element):
        (key, values) = element
        return [(key, sum(values))]


def filter_defaulters(element):  # 3 or more late payments
    (key, n_late_payments) = element
    return element[1] >= 3


p1 = beam.Pipeline()

med_loan_defaulters = (
                    p1
                    | beam.io.ReadFromText('loan.txt',
                                           skip_header_lines=1)
                    | beam.Map(split_row)
                    # | beam.Map(print)
                    | beam.Map(remove_spaces)
                    # | beam.Map(print)
                    | beam.Filter(filter_medical_loans)
                    # | beam.Map(print)
                    | beam.Map(add_points_column)
                    | beam.Map(late_payments)
                    # | beam.Map(print)
                    | beam.Map(get_select_elements)  # ('CT55975', 1), ('CT55975', 0)
                    # | beam.Map(print)
                    | beam.GroupByKey()  # ('CT55975', [0, 0, 0, 1, 1, 1, 0, 1, 0, 0, 1, 0])
                    | beam.ParDo(Counting())  # ('CT55975', 5)
                    | beam.Filter(filter_defaulters)
                    | beam.Map(print)
                      )

p1.run()
