import csv
import json


files = ['sample/ltr/claim_header.txt', 'sample/ltr/claim_edits.txt', 'sample/ltr/claim_detail.txt']

claims = {}

with open(files[0], encoding='utf-16') as csvfile:
    reader = csv.DictReader(csvfile)
    title = reader.fieldnames
    for row in reader:
        claims[row[title[0]]] = {'header': {title[i]:row[title[i]] for i in range(len(title))}}
        # claims[row[0]] = [{title[i]:row[title[i]] for i in range(len(title))}]
        # csv_rows.extend([{title[i]:row[title[i]] for i in range(len(title))}])

edits = {}
with open(files[1], encoding='utf-16') as csvfile:
    reader = csv.DictReader(csvfile)
    title = reader.fieldnames
    for row in reader:
        if edits.get(row[title[0]]) is not None:
            edits.get(row[title[0]]).extend([{title[i]:row[title[i]] for i in range(len(title))}])
        else:
            edits[row[title[0]]] = [{title[i]:row[title[i]] for i in range(len(title))}]

details = {}
with open(files[2], encoding='utf-16') as csvfile:
    reader = csv.DictReader(csvfile)
    title = reader.fieldnames
    for row in reader:
        if details.get(row[title[0]]) is not None:
            details.get(row[title[0]]).extend([{title[i]:row[title[i]] for i in range(len(title))}])
        else:
            details[row[title[0]]] = [{title[i]:row[title[i]] for i in range(len(title))}]

print (len(claims.keys()))
print(edits['19016MC2715'])
print(len(edits['19016MC2715']))
print(len(details['19016MC2715']))

f = open('sample/ltr/formatted_input.generated', 'w')

for key in claims.keys():
    obj = claims.get(key,  {})
    obj['edits'] = edits.get(key, [])
    obj['details'] = details.get(key, [])
    f.write(json.dumps(obj) + '\n')