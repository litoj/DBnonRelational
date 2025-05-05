import time
from generator import generate
import ansatz_0, ansatz_1, ansatz_2

# Benchmark parameters
MATRIX_SIZES = [8, 16, 32, 64, 128, 256]
SPARSITIES = [0.1, 0.3, 0.5, 0.7, 0.9]
REPETITIONS = 5

results = []


for size in MATRIX_SIZES:
    for sparsity in SPARSITIES:
        sparsity_str = str(sparsity).replace(".", "")
        # Generate test matrices
        generate(f"bench_{size}_{sparsity_str}", size, sparsity)

        # Time each approach
        for approach in [ansatz_0, ansatz_1, ansatz_2]:
            times = []
            for _ in range(REPETITIONS):
                if approach.__name__ == "ansatz_0":
                    start = time.perf_counter()
                    approach.client_side_matmul(
                        f"bench_{size}_{sparsity_str}_h",
                        f"bench_{size}_{sparsity_str}_v",
                        f"result_{approach.__name__}",
                    )
                elif approach.__name__ == "ansatz_1":
                    start = time.perf_counter()
                    approach.sql_side_matmul(
                        f"bench_{size}_{sparsity_str}_h",
                        f"bench_{size}_{sparsity_str}_v",
                        f"result_{approach.__name__}",
                    )
                elif approach.__name__ == "ansatz_2":
                    approach.convert_to_vector(
                        f"bench_{size}_{sparsity_str}_h",
                        "row",
                    )
                    approach.convert_to_vector(
                        f"bench_{size}_{sparsity_str}_v",
                        "col",
                    )
                    start = time.perf_counter()
                    approach.vector_matmul(
                        f"bench_{size}_{sparsity_str}_h_vector",
                        f"bench_{size}_{sparsity_str}_v_vector",
                        f"result_{approach.__name__}",
                    )
                end = time.perf_counter()
                times.append(end - start)

            median_time = sorted(times)[len(times) // 2]
            results.append(
                {
                    "size": size,
                    "sparsity": sparsity,
                    "approach": approach.__name__,
                    "time": median_time,
                }
            )

# Save results to CSV
import csv

with open("benchmark_results.csv", "w") as f:
    writer = csv.DictWriter(f, fieldnames=["size", "sparsity", "approach", "time"])
    writer.writeheader()
    writer.writerows(results)
