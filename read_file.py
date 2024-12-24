import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Define Pipeline Options
options = PipelineOptions()

# List of column names
columns = [
    "CLIENTNUM", "Attrition_Flag", "Customer_Age", "Gender", "Dependent_count", 
    "Education_Level", "Marital_Status", "Income_Category", "Card_Category", 
    "Months_on_book", "Total_Relationship_Count", "Months_Inactive_12_mon", 
    "Contacts_Count_12_mon", "Credit_Limit", "Total_Revolving_Bal", 
    "Avg_Open_To_Buy", "Total_Amt_Chng_Q4_Q1", "Total_Trans_Amt", 
    "Total_Trans_Ct", "Total_Ct_Chng_Q4_Q1", "Avg_Utilization_Ratio", 
    "Naive_Bayes_Classifier_Attrition_Flag_Card_Category_Contacts_Count_12_mon_Dependent_count_Education_Level_Months_Inactive_12_mon_1", 
    "Naive_Bayes_Classifier_Attrition_Flag_Card_Category_Contacts_Count_12_mon_Dependent_count_Education_Level_Months_Inactive_12_mon_2"
]

def combine_keys_values(record):
    combine = ""
    length = len(record.values())
    index = 0
    for value in record.values():
        if(index == length - 1):
            combine += value
        else:
            combine += value + ','
        index += 1
    #print(combine)
    return combine
    

def run():    
    # Define the pipeline
    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | 'Read CSV File' >> beam.io.ReadFromText(r'input\BankChurners.csv', skip_header_lines = 0, coder=beam.coders.StrUtf8Coder())
            | 'Split Columns' >> beam.Map(lambda x: x.split(','))
            | 'Map Rows to Dict' >> beam.Map(lambda values: dict(zip(columns, values)))  # Convert rows to dictionaries
            | 'Filter Married' >> beam.Filter(lambda record: record['Marital_Status'].lower()  == '"married"' or record['Marital_Status'] == '"Marital_Status"')
            #| 'Debug' >> beam.Map(print)
            | 'Format as CSV' >> beam.Map(combine_keys_values)
            | 'Write to File' >> beam.io.WriteToText('output/write', file_name_suffix='.csv', shard_name_template="-SSSS", num_shards=1)
        )

    print('Pipeline is completed')

if __name__ == '__main__':
    run()