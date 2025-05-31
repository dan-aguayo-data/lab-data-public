import random

import textart_blackjack_logo
from time import sleep


print(textart_blackjack_logo.blackjack_logo())


cards = [11,1,2,3,4,5,6,7,8,9,10,10,10,10]   #equal chance of occurring, no depleting from a deck

def deal_card():
    """Returns a random card from the deck"""
    cards = [11, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10, 10, 10]
    card = random.choice(cards)
    return card


def calculate_score(cards):
    """Take a list of cards and return the score calculated by the cards"""
    if sum(cards) == 21 and len(cards) == 2:
        return 0
    if 11 in cards and sum(cards) > 21:
        cards.remove(11)
        cards.append(1)
    return sum(cards)  #this can sum the items of a list one by one

def compare(u_score, c_score): #elif is evaluated from top to bottom
    if u_score == c_score:
        return "Draw"
    elif c_score == 0:
        return "Lose, opponent has Blackjack"
    elif u_score > 21:
        return "You went over. You lose."
    elif u_score > c_score:
        return "You win"
    else:
        return "You lose"

def play_game():
    print(textart_blackjack_logo.blackjack_logo())
    user_cards = []
    computer_cards = []
    computer_score = -1
    user_score = -1
    is_game_over = False

    for _ in range(2):
        user_cards.append(deal_card()) # when you want to add a single item, not a list to an existing list you have to use append
        computer_cards.append(deal_card())

    while not is_game_over:
        user_score = calculate_score(user_cards)
        computer_score = calculate_score(computer_cards)
        print(f"Your cards: {user_cards}, current score: {user_score}")
        sleep(2)
        print(f"Computer first card: [{computer_cards[0]}]")
        sleep(2)
        print('\n' * 1)
        if user_score == 0 or computer_score == 0 or user_score > 21:
            is_game_over = True
        else:
            user_should_deal = input("Type 'y' to get another card, type 'n' to pass: ")
            if user_should_deal == "y":
                user_cards.append(deal_card())
            else:
                is_game_over = True

    print('\n'*1)
    sleep(2)
    print(f"Computer's cards: {computer_cards}, computer's score:  {computer_score}'")
    sleep(5)
    print('\n'*1)

    while computer_score != 0 and computer_score < 17:
        print(f"Computer gets another card")
        sleep(2)
        print('\n' * 1)
        computer_cards.append(deal_card())
        computer_score = calculate_score(computer_cards)
        sleep(3)
        print(f"Computer cards: {computer_cards}, computer's score:  {computer_score}")

    print('\n' * 1)
    print(f"Your final hand: {user_cards}, final score: {user_score}")
    print(f"Computer's final hand: {computer_cards}, final score: {computer_score}")
    print('\n' * 1)
    sleep(2)
    print(compare(user_score,computer_score))

while input("Do you want to play a game of Blackjack? type 'y' or 'n' ") == 'y':
    play_game()