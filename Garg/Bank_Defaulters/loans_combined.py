import apache_beam as beam

######### Common helper functions ##########


def split_row(element):
    return element.split(',')


def remove_spaces(element):  # Note: Some columns values start with a space
    element[6] = element[6].replace(" ", "")  # ' 30-01-2018' --> '30-01-2018'
    element[8] = element[8].replace(" ", "")
    return element


def get_select_elements(record):
    selected_elements = (record[0], record[9])  # convert to tuple, in order to do GroupBy later
    return selected_elements


######### Loan-specific helper functions ########## personal-loans

def filter_personal_loans(element):
    return element[5] == "Personal Loan"


def add_month_column(element):
    payment_date = element[-1]  # '26-02-2018'
    payment_date = payment_date.split('-')  # ['26', '02', '2018']
    payment_month = payment_date[1]  # '02'
    payment_month = int(payment_month)  # 2
    element.append(payment_month)  # default value for no. of late_payments
    return element


def get_max_gap(months):  # [1, 2, 3, 4, 6]
    gap = []
    for item in range(len(months) - 1):
        gap.append(months[item + 1] - months[item])
        # [1, 1, 1, 2]
    if months[-1] < 12:  # Last month may not be December
        gap.append(12 - months[-1])  # [1, 1, 1, 2, 6]
    return max(gap)  # 6


def count_missed_payments_n_max_gap(element):
    customer_id, months = element
    element = list(element)
    element.append(12 - len(months))  # no. of missed instalments
    element.append(get_max_gap(months))  # max_gap
    return element


def check_whether_defaulter(element):
    element.append(0)
    if element[2] >= 4 or element[3] >= 2:
        element[4] = 1
    return element


def filter_per_loan_defaulters(element):
    (customer_id, months, missed_instalments, max_gap, defaulter) = element
    return element[-1] == 1


############# Medical-loan helper functions ##############

def filter_medical_loans(element):
    return element[5] == "Medical Loan"


def add_points_column(element):  # penalty points
    element.append(0)  # default value for no. of late_payments
    return element


def late_payments(element):
    if element[8] > element[6]:
        element[9] += 1  # 1 point for each penalty
    return element


class Counting(beam.DoFn):

    def process(self, element):
        (key, values) = element
        return [(key, sum(values))]


def filter_med_loan_defaulters(element):  # 3 or more late payments
    (key, n_late_payments) = element
    return element[1] >= 3


########### Build pipeline ###########

p1 = beam.Pipeline()

# common transformations in the pipeline
input_collection = (
                    p1
                    | beam.io.ReadFromText('input/loan.txt',
                                           skip_header_lines=1)
                    | beam.Map(split_row)
                    | beam.Map(remove_spaces)
                    )

# loan-specific transformations

required_columns = "customer_id,instalments_paid_months,total_missed_payments,consecutive_missed_payments,whether_defaulter"

personal_loan_defaulters = (
                    input_collection
                    | beam.Filter(filter_personal_loans)
                    | beam.Map(add_month_column)
                    | "for per_loan" >> beam.Map(get_select_elements)  # ('CT63413', 1), ('CT63413', 2)
                    | "per_loan" >> beam.GroupByKey()  # ('CT63413', [1, 2, 3, 4, 5, 6, 7])
                    | beam.Map(count_missed_payments_n_max_gap)  # ['CT63413', [1, 2, 3, 4, 5, 6, 7], 5, 5]
                    | beam.Map(check_whether_defaulter)  # ['CT63413', [1, 2, 3, 4, 5, 6, 7], 5, 5, 1]
                    | beam.Filter(filter_per_loan_defaulters)
                    # | beam.Map(print)
                    | "per_loan defaulters list" >> beam.io.WriteToText('output/personal_loan_defaulters',
                                                                        header=required_columns)
                           )

medical_loan_defaulters = (
                    input_collection
                    | beam.Filter(filter_medical_loans)
                    | beam.Map(add_points_column)
                    | beam.Map(late_payments)
                    | "for med_loan" >> beam.Map(get_select_elements)  # ('CT55975', 1), ('CT55975', 0)
                    | "med_loan" >> beam.GroupByKey()  # ('CT55975', [0, 0, 0, 1, 1, 1, 0, 1, 0, 0, 1, 0])
                    | beam.ParDo(Counting())  # ('CT55975', 5)
                    | beam.Filter(filter_med_loan_defaulters)
                    # | beam.Map(print)
                    | "med_loan defaulters list" >> beam.io.WriteToText('output/medical_loan_defaulters',
                                                                        header="customer_id,n_late_payments")
                          )

p1.run()
