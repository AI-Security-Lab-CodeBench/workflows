import csv
import io

if __package__:
    from .dataset_schema import FIELDS, DatasetError
else:
    from dataset_schema import FIELDS, DatasetError


def validate_csv_quoting(text):
    state = "start"
    for character in text:
        if state == "quoted":
            if character == '"':
                state = "after_quote"
        elif state == "after_quote":
            if character == '"':
                state = "quoted"
            elif character in ",\r\n":
                state = "start"
            else:
                raise DatasetError("CSV has text after a closing quote")
        elif character == '"':
            if state != "start":
                raise DatasetError("CSV has an unescaped quote inside an unquoted field")
            state = "quoted"
        elif character in ",\r\n":
            state = "start"
        else:
            state = "unquoted"
    if state == "quoted":
        raise DatasetError("CSV has an unterminated quoted field")


def read_csv(path):
    try:
        raw = path.read_bytes()
        text = raw.decode("utf-8", errors="strict")
        validate_csv_quoting(text)
        records = list(csv.reader(io.StringIO(text, newline=""), strict=True))
    except (OSError, UnicodeError, csv.Error) as error:
        raise DatasetError("Cannot read commands.csv as strict UTF-8 CSV") from error
    if not records or records[0] != list(FIELDS):
        raise DatasetError("commands.csv header must match exact nine-field contract")
    if any(len(record) != len(FIELDS) for record in records[1:]):
        raise DatasetError("commands.csv contains a data record with incorrect width")
    stream = io.StringIO(newline="")
    csv.writer(stream).writerows(records)
    if list(csv.reader(io.StringIO(stream.getvalue(), newline=""), strict=True)) != records:
        raise DatasetError("CSV parsed-value roundtrip failed")
    return [dict(zip(FIELDS, record)) for record in records[1:]]
