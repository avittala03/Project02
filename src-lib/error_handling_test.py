from jsonschema import validate
import jsonschema
import json

def main():
        file = open("C:\\Users\\AG05216\\PycharmProjects\\ds-cogx-api\\sample\\um\\SAMPLE_PAYLOAD.txt", "r")
        file1= file.readlines()
        json_schema = open("C:\\Users\\AG05216\\PycharmProjects\\ds-cogx-api\\sample\\um\\UM_Schema.json", "r")
        schema = json.load(json_schema)
        # print("schema", schema)
        request_number = 0
        f_valid = open("C:\\Users\\AG05216\\PycharmProjects\\ds-cogx-api\\sample\\um\\ValidPayload.txt", "w")
        f_err = open("C:\\Users\\AG05216\\PycharmProjects\\ds-cogx-api\\sample\\um\\ErrorMessages.txt", "w")
        for line in file1:
            data = json.loads(line)
            request_number = request_number + 1
            v = jsonschema.Draft7Validator(schema).is_valid(data, schema)
            if (v == True):
                #print("valid request number:", request_number)
                #valid records are written to ValidPayload.txt
                f_valid.write("\n")
                f_valid.write(json.dumps(data))
            else:
                z = jsonschema.Draft7Validator(schema)
                for error in sorted(z.iter_errors(data), key=str):
                    f_err.write("invalid request number:" + str(request_number))
                    f_err.write("\n")
                    f_err.write(str(error.message))
                    f_err.write("\n")
main()
