import random
#
#  _
# | |
# | |__   __ _ _ __   __ _ _ __ ___   __ _ _ __
# | '_ \ / _` | '_ \ / _` | '_ ` _ \ / _` | '_ \
# | | | | (_| | | | | (_| | | | | | | (_| | | | |
# |_| |_|\__,_|_| |_|\__, |_| |_| |_|\__,_|_| |_|
#                     __/ |
#                    |___/

import random



word_list = [
    "antidisestablishmentarianism",
    "floccinaucinihilipilification",
    "pneumonoultramicroscopicsilicovolcanoconiosis",
    "sesquipedalian",
    "supercalifragilisticexpialidocious",
    "honorificabilitudinitatibus",
    "incomprehensibilities",
    "hippopotomonstrosesquipedaliophobia",
    "pulchritudinous",
    "quixotic"
]

chosen_word = random.choice(word_list)
# ... [rest of your code remains unchanged]

hangmanpics = ['''
  +---+
  |   |
      |
      |
      |
      |
=========''', '''
  +---+
  |   |
  O   |
      |
      |
      |
=========''', '''
  +---+
  |   |
  O   |
  |   |
      |
      |
=========''', '''
  +---+
  |   |
  O   |
 /|   |
      |
      |
=========''', '''
  +---+
  |   |
  O   |
 /|\  |
      |
      |
=========''', '''
  +---+
  |   |
  O   |
 /|\  |
 /    |
      |
=========''', '''
  +---+
  |   |
  O   |
 /|\  |
 / \  |
      |
=========''']

chosen_word = random.choice(word_list)
print(chosen_word)
word_progress = ""
hangman_progress = 0
word_progress = ""
correct_indicator = 0
chosen_letters = []
correct_letters = []
START = "Y"
for letter in chosen_word:
    word_progress = word_progress + "_"

length_chosen_word = int(len(chosen_word))
# print(len(chosen_word))



print("Welcome to the hangman game!\n")

print(r"""
_
| |
| |__   __ _ _ __   __ _ _ __ ___   __ _ _ __
| '_ \ / _` | '_ \ / _` | '_ ` _ \ / _` | '_ \
| | | | (_| | | | | (_| | | | | | | (_| | | | |
|_| |_|\__,_|_| |_|\__, |_| |_| |_|\__,_|_| |_|
                        __/ |
                       |___/

""")
while START == "Y":

    START = input("Would you like to start a new game? Y/N \n").upper()

    if START == 'Y':
        print(f"This is the length of the letter to guess :  {length_chosen_word}\n")
        while word_progress.find("_") != -1 :
            print(hangmanpics[hangman_progress] + "\n")
            chosen_letters = list(set(chosen_letters))
            correct_letters = list(set(correct_letters))
            print(f"Your progress: {word_progress}\n")
            guess = input("Guess a letter: ").lower()
            while guess in chosen_letters:
                print("You have already selected that letter\n")
                guess = input("Select another letter: ").lower()
            chosen_letters += guess
            choice_evaluation = ""
            correct_indicator = 0
            for position in range (0,length_chosen_word):
                if chosen_word[position] == guess:
                    correct_indicator += 1
                    choice_evaluation = choice_evaluation + chosen_word[position]
                    correct_letters += chosen_word[position]
                else:
                    choice_evaluation = choice_evaluation + word_progress[position]

            if correct_indicator > 0:
                print("\n" + "Well done! You guessed a letter!\n")
            else:
                print("\n"+"You guessed the wrong letter!\n")
                hangman_progress += 1
            word_progress = choice_evaluation
            if hangman_progress == 6:
                word_progress = ""
                print("\n"+"You have lost the game! \nBetter luck next time!\n")
            else:
                print("You won the game! \n")


    else:
        print ("Goodbye!")


