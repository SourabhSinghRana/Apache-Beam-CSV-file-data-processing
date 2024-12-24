# Apache-Beam-CSV-file-data-processing
✅ Read data from a CSV file. ✅ Filtered the data based on specific criteria. ✅ Saved the processed data back into a CSV file.


🚀 Exploring Apache Beam for Data Processing

Recently, I worked on a project where I utilized Apache Beam to process data efficiently. Here's a quick overview of what I accomplished:

✅ Read data from a CSV file.
✅ Filtered the data based on specific criteria.
✅ Saved the processed data back into a CSV file.

About Apache Beam
Apache Beam is an open-source, unified model for defining batch and streaming data processing pipelines. It allows you to write your pipeline once and execute it on different runners, such as Apache Spark, Google Dataflow, or Flink.

Here are the key concepts I used in my code:
1️⃣ Pipeline: Represents the data workflow.
2️⃣ PCollection: Beam's distributed dataset abstraction.
3️⃣ Transforms: Operations applied to the data, such as Map, Filter, and Write.
4️⃣ I/O Connectors: Read and write data to/from various formats and storage systems.

Portability: The same code can run on multiple execution engines.
Scalability: Perfect for batch and stream processing.
Flexibility: Easy to integrate with various data sources and sinks.
💡 This project was a great hands-on experience and further solidified my understanding of data pipelines and distributed processing frameworks.

I'm excited to continue exploring Apache Beam and other big data tools for building efficient data processing solutions.
Let's connect and exchange ideas if you're also working on data engineering or big data projects!

#ApacheBeam #DataEngineering #BigData #Python



# Read the CSV file and skip the header
| 'Read CSV File' >> beam.io.ReadFromText(r'input\BankChurners.csv', skip_header_lines=0)

# Split each line into a list of values based on commas
| 'Split Columns' >> beam.Map(lambda x: x.split(','))

# Convert each row to a dictionary using column names
| 'Map Rows to Dict' >> beam.Map(lambda values: dict(zip(columns, values)))

# Filter rows where 'Marital_Status' is "married"
| 'Filter Married' >> beam.Filter(lambda record: record['Marital_Status'].lower() == '"married"' or record['Marital_Status'] == '"Marital_Status"')

# Format the dictionary back into a CSV row string
| 'Format as CSV' >> beam.Map(combine_keys_values)

# Write the filtered data to an output CSV file
| 'Write to File' >> beam.io.WriteToText('output/write', file_name_suffix='.csv', shard_name_template="-SSSS", num_shards=1)
