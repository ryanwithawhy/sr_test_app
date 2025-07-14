def generate_records(record_count, field_count, ):
    records = []
    for x in range(1, record_count, 1):
        record = {"id": x}
        if field_count > 1:
            record["email"] = f"user{x}@email.com"
        if field_count > 2:
            record["authenticated"] = True
        records.append(record)
    return records

if __name__ == "__main__":
    generate_records(10, 2)


