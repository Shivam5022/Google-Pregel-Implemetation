import matplotlib.pyplot as plt

# Given data
vertices = [100, 1000, 5000, 10000, 50000]
time_taken = [1168, 3431, 13337, 26008, 109847]

# Plotting the graph with lines and markers
plt.figure(figsize=(8, 6))
plt.plot(vertices, time_taken, marker='o', linestyle='-', color='b', markersize=8)
plt.title('Number of Vertices vs Time Taken (Pagerank Computation)')
plt.xlabel('Number of Vertices')
plt.ylabel('Time Taken (ms)')
# plt.grid(True)
plt.xscale('log')  # Set x-axis to logarithmic scale if vertices are widely spaced

# Marking each x-coordinate
for i in range(len(vertices)):
    plt.text(vertices[i], time_taken[i], str(vertices[i]), ha='right', va='bottom')

# Show the plot
plt.show()