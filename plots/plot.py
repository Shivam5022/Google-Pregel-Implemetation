# import matplotlib.pyplot as plt

# # Given data
# vertices = [1000, 10000, 50000, 100000, 500000]
# time_taken = [1168, 3431, 13337, 26008, 109847]

# # Plotting the graph with lines and markers
# plt.figure(figsize=(8, 6))
# plt.plot(vertices, time_taken, marker='o', linestyle='-', color='b', markersize=8)
# plt.title('Number of Vertices vs Time Taken (Pagerank Computation) for 5 workers')
# plt.xlabel('Number of Vertices')
# plt.ylabel('Time Taken (ms)')
# # plt.grid(True)
# plt.xscale('log')  # Set x-axis to logarithmic scale if vertices are widely spaced

# # Marking each x-coordinate
# for i in range(len(vertices)):
#     plt.text(vertices[i], time_taken[i], str(vertices[i]), ha='right', va='bottom')

# # Show the plot
# plt.show()

import matplotlib.pyplot as plt

# Given data
workers = [2, 3, 4, 5]
time_taken = [17938, 14928, 13889, 13029]

# Plotting the graph
plt.figure(figsize=(8, 6))
plt.plot(workers, time_taken, marker='o', linestyle='-', color='b', markersize=8)
plt.title('Number of Workers vs Time Taken (Pagerank for 50000 vertices)')
plt.xlabel('Number of Workers')
plt.ylabel('Time Taken (ms)')

# Set x-axis ticks to only display the provided worker values
plt.xticks(workers)

# Show the plot
plt.show()
