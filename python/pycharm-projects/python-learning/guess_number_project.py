import random
from art import text2art
import pyfiglet


def match_indicator_attempts(match_indicator_input,num_attempts_input,secret_number_input):
    while match_indicator_input == False and num_attempts_input > 0:
        guess = int(input("Make a guess: "))
        if guess >= 1 and guess <= 100:
            guess_in_range = True
        else:
            guess_in_range = False
        if guess < secret_number_input and guess_in_range == True:
            print("Too low.")
            print("Guess again.")
            print('\n' * 1)
            num_attempts_input -= 1
            print(f"You have {num_attempts_input} attempts remaining to guess the number.")
        elif guess > secret_number_input and guess_in_range == True:
            print("Too High.")
            print("Guess again.")
            print('\n' * 1)
            num_attempts_input -= 1
            print(f"You have {num_attempts_input} attempts remaining to guess the number.")
        elif guess == secret_number_input:
            match_indicator_input = True
            print(f"You got it! The answer was {secret_number_input}")
        else:
            print("Your guess is not in the range specified.")
            print("Guess again between numbers 1 and 100.")
            num_attempts_input -= 1
            print(f"You have {num_attempts_input} attempts remaining to guess the number.")
            print('\n' * 1)


def play_guess_the_game():
    continue_playing = True
    while continue_playing == True:
        guess_text = text2art("Guess the number", font="slant")
        print(guess_text)
        print("Welcome to the Number Guessing Game!")
        print("I am thinking about a number between 1 and 100")
        difficulty = input("Choose a difficulty. Type 'easy' or 'hard': ")
        num_attempts = 0
        match_indicator = False
        guess_in_range = True
        if difficulty.lower() == 'hard':
            secret_number = random.randrange(1,100)
            num_attempts = 5
            print(f"You have {num_attempts} attempts remaining to guess the number.")
            match_indicator_attempts(match_indicator,num_attempts,secret_number)
        elif difficulty.lower() == 'easy':
            secret_number = random.randrange(1,100)
            num_attempts = 10
            print(f"You have {num_attempts} attempts remaining to guess the number.")
            match_indicator_attempts(match_indicator,num_attempts,secret_number)
        else:
            print("You can only select between 'easy' or 'hard'. Please make sure you enter either to start the game.")
            play_guess_the_game()
        continue_game = input("Do you want to continue playing 'y' or 'n': ")
        if continue_game == 'y':
            continue_playing == True
            print('\n' * 100)
        else:
            continue_playing == False

play_guess_the_game()
