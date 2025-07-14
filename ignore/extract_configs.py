import re
import csv
from collections import Counter

# Read the text file
with open('formats_etc.txt', 'r') as f:
    text = f.read()

# Step 1: Extract words with at least one period
words_with_dots = re.findall(r'\b[\w.]*\.[\w.]*\b', text)

# List of exact words to exclude
# Step 2 Filter out words 
excluded_words = {
    'i.e', 
    'topic.rename.format', 
    'google.protobuf.Timestamp',
    'org.apache.kafka.common.serialization.StringDeserializer',
    'creds.config',
    'org.apache.avro.AvroRuntimeException',
    'org.apache.kafka.common.security.plain.PlainLoginModule'
    }  # <-- Add more here as needed
words_filtered = [w for w in words_with_dots if w not in excluded_words]

# Step 3: Remove anything that starts with: io, java, com, test, or any number
excluded_prefixes = ('io', 'java', 'com', 'test')
words_filtered = [
    w for w in words_filtered
    if not w.startswith(excluded_prefixes) and not re.match(r'^\d', w)
]

# Step 4: Count occurrences
counts = Counter(words_filtered)

# Step 5: Write to CSV
with open('output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    for word, count in counts.items():
        writer.writerow([word, count])