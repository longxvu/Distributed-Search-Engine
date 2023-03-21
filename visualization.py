import matplotlib.pyplot as plt
import numpy as np

term_distributed_1 = np.array([27.517, 20.539, 19.350, 20.098, 21.151])
term_distributed_2 = np.array([28.002, 22.225, 21.430, 21.020, 22.329])
query_distributed_1 = np.array([13.37, 9.531, 9.090, 9.193, 10.378])
query_distributed_2 = np.array([13.409, 10.422, 9.625, 9.731, 11.086])
n_processes = list(range(1, 6))

plt.figure()
plt.fill_between(n_processes, term_distributed_1, term_distributed_2, alpha=.5, linewidth=0)
plt.plot(n_processes, (term_distributed_1 + term_distributed_2) / 2, linewidth=2, label="Term distributed")

plt.fill_between(n_processes, query_distributed_1, query_distributed_2, alpha=.5, linewidth=0)
plt.plot(n_processes, (query_distributed_1 + query_distributed_2) / 2, linewidth=2, label="Query distributed")

plt.xticks(list(np.arange(1, 6)))
plt.xlabel("Number of processes")
plt.ylabel("Execution time")
plt.title("Performance comparison without cache")
plt.legend()
plt.savefig("no_cache.png")
plt.show()


term_distributed_cache_1 = np.array([9.684, 7.559, 7.271, 6.817, 7.449])
term_distributed_cache_2 = np.array([10.017, 8.242, 7.552, 7.738, 7.732])
query_distributed_cache_1 = np.array([12.841, 9.525, 9.842, 9.531, 10.531])
query_distributed_cache_2 = np.array([12.966, 10.278, 10.523, 9.817, 10.576])

plt.figure()
plt.fill_between(n_processes, term_distributed_cache_1, term_distributed_cache_2, alpha=.5, linewidth=0)
plt.plot(n_processes, (term_distributed_cache_1 + term_distributed_cache_2) / 2, linewidth=2, label="Term distributed")

plt.fill_between(n_processes, query_distributed_cache_1, query_distributed_cache_2, alpha=.5, linewidth=0)
plt.plot(n_processes, (query_distributed_cache_1 + query_distributed_cache_2) / 2, linewidth=2, label="Query distributed")

plt.xticks(list(np.arange(1, 6)))
plt.xlabel("Number of processes")
plt.ylabel("Execution time")
plt.title("Performance comparison with cache")
plt.legend()
plt.savefig("with_cache.png")
plt.show()