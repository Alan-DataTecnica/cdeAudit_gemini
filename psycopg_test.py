import psycopg2
import pandas as pd
from io import StringIO
import time

conn = psycopg2.connect(host="localhost", database="cde-review-dev", user="postgres", password="\\^OVI3yM]V<8<tqK")

# Test larger batches to find the breaking point
for batch_size in [1000, 5000, 10000, 20000]:
    print(f"Testing {batch_size} records...")
    
    test_data = []
    for i in range(batch_size):
        test_data.append([f"ID_{i}", f"Title_{i}", f"Variable_{i}"])
    
    df = pd.DataFrame(test_data, columns=['id', 'title', 'variable'])
    
    cur = conn.cursor()
    cur.execute(f"CREATE TEMP TABLE copy_test_{batch_size} (id TEXT, title TEXT, variable TEXT);")
    
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, sep='\t')
    buffer.seek(0)
    
    try:
        start = time.time()
        cur.copy_expert(f'COPY copy_test_{batch_size} FROM STDIN WITH CSV DELIMITER E\'\\t\'', buffer)
        conn.commit()
        duration = time.time() - start
        print(f"✅ {batch_size} records successful in {duration:.2f}s")
    except Exception as e:
        print(f"❌ {batch_size} records failed: {e}")
        break
    
    cur.close()

conn.close()