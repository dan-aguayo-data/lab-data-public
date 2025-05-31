# Formula when working with negatives
#     -100 % 3
#     -100 - (3 * floor(-100 / 3))

# Circular Indexing (Avoiding Out-of-Bounds in Arrays)


arr = [10, 20, 30]
index = -100 % 3  # This gives 2, so arr[2] is valid
print(arr[index])  # Output: 30





# Handling Negative Steps in Rotations
# Problem: You're implementing a circular buffer (like a playlist or a clock) and need to rotate in both directions.
#
#

n = 12  # 12-hour clock
current_hour = 3  # Start at 3 o’clock

back_100_hours = (current_hour + 2) % n
print(back_100_hours)  # Output: 7


#  Keeping Remainders Always Positive (Consistent Hashing)
# Problem: When implementing hash functions or partitioning data, you want the remainder to always be non-negative.


def positive_mod(n, mod):
    return (n % mod + mod) % mod  # Ensures positive remainder

print(positive_mod(-100, 3))  # Output: 2


# Finding the Last k Elements in a Sequence
# Problem: You need to find the last k elements but k could be larger than the length of the sequence.

n = 7  # Total elements
k = 100  # We want the last 100 elements

start_index = -k % n  # Wraps around instead of going negative
print(start_index)  # Output: 4 (Starts at index 4 in a circular array)


# Working with Timestamps (Modulo in Time Calculation)

current_time = 23  # 11 PM
next_hour = (current_time + 100) % 24
print(next_hour)  # Output: 3 AM