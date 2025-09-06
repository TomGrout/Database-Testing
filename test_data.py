def generate_test_data(count):
    docs = []
    for i in range(count):
        docs.append({
            '_id': i,
            'secondaryField': f'value-{i}',
            'nonIndexedField': f'non-indexed-{i}',
            'sparseField': f'sparse-{i}' if i % 2 == 0 else None,
            'dataForJoin': i % 100
        })
    return docs