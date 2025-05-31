
print("--\n--\n---\n")

print("Welcome to the auction program!")
print("\n" * 100) # this is to create 100 lines


def find_highest_bidder(bidding_dictionary):
    winner = ""
    highest_bid = 0
    for bidder in bidding_dictionary:
        bid_amount = bidding_dictionary[bidder]
        if bid_amount > highest_bid:
            highest_bid = bid_amount
            winner = bidder
    print(f"The winner is {bidder}!")
#Ask the user for inputs

bids = {}
continue_bidding = True

while continue_bidding:
    name = input("What is your name?\n")
    price = float(input("What is your bid? \n$"))
    bids[name] = price
    should_continue = input("Are there other bidders? Yes / No \n").lower()
    if should_continue == "no":
        continue_bidding = False
        find_highest_bidder(bids)


