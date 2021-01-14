import csv
import json


files = ['sample/um/claim_line.txt']
claim_lines = {}
with open(files[0], encoding='ISO-8859-1') as csvfile:
    reader = csv.DictReader(csvfile)
    title = reader.fieldnames
    for row in reader:
        if claim_lines.get(row[title[0]]+row[title[6]], None) == None:
            pass
        else:
            print(row[title[0]])
        claim_lines[row[title[0]]+row[title[6]]] = {title[i]:row[title[i]] for i in range(len(title))}

print(len(claim_lines.keys()))

f = open('sample/um/formatted_input.generated', 'w')

for key in claim_lines.keys():
    obj = {}
    obj['um_claim'] = claim_lines.get(key,  {})
    f.write(json.dumps(obj) + '\n')