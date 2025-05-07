import time
from generator import create_table, generate
import ansatz_0, ansatz_1, ansatz_2

# Benchmark parameters
MATRIX_SIZES = [8, 16, 32, 64, 128, 256]
SPARSITIES = [0.1, 0.3, 0.5, 0.7, 0.9]
REPETITIONS = 40

results = []


for size in MATRIX_SIZES:
    for sparsity in SPARSITIES:
        sparsity_str = str(sparsity).replace(".", "")
        # Generate test matrices
        base_name = f"bench_{size}_{sparsity_str}"
        generate(base_name, size, sparsity)
        h_tbl = base_name + "_h"
        v_tbl = base_name + "_v"

        # Time each approach
        for approach in [ansatz_0, ansatz_1, ansatz_2]:
            times = []
            res_tbl = f"result_{approach.__name__}"

            start, A, B = time.perf_counter(), None, None
            create_table(res_tbl)
            if approach.__name__ == "ansatz_0":
                A = approach.get_data(h_tbl)
                B = approach.get_data(v_tbl)
            elif approach.__name__ == "ansatz_2":
                approach.convert_to_vector(
                    h_tbl,
                    "row",
                )
                approach.convert_to_vector(
                    v_tbl,
                    "col",
                )
            overhead = time.perf_counter() - start

            for _ in range(REPETITIONS):
                start = time.perf_counter()
                if approach.__name__ == "ansatz_0":
                    approach.client_side_matmul(
                        A,
                        B,
                        res_tbl,
                    )
                elif approach.__name__ == "ansatz_1":
                    approach.sql_side_matmul(
                        h_tbl,
                        v_tbl,
                        res_tbl,
                    )
                elif approach.__name__ == "ansatz_2":
                    approach.vector_matmul(
                        h_tbl + "_vector",
                        v_tbl + "_vector",
                        res_tbl,
                    )
                times.append(time.perf_counter() - start)

            avg_time = sum(times) / len(times)
            results.append(
                {
                    "size": size,
                    "sparsity": sparsity,
                    "approach": approach.__name__,
                    "time": avg_time,
                    "overhead": overhead,
                }
            )

# Save results to CSV
import csv

# print(results)
with open("benchmark_results.csv", "w") as f:
    writer = csv.DictWriter(
        f, fieldnames=["size", "sparsity", "approach", "time", "overhead"]
    )
    writer.writeheader()
    writer.writerows(results)
